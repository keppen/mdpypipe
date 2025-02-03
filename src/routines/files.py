from copy import deepcopy
import logging
import os
import re
from pathlib import Path
import shutil
from subprocess import CompletedProcess
import sys
from typing import override

import parmed as pmd

from src.context.context import MDContext
from src.interfaces.interfaces import ShellInterface
from src.interfaces.pipeline import NextStep, PipeStep
from src.logger import log_json


class ObabelShell(ShellInterface):
    def __init__(self, molecule_name: str) -> None:
        self.molecule_name: str = molecule_name
        self.step_name: list[str] = ["OBABELRUN", self.molecule_name]

    @override
    def __call__(self, context: MDContext, next_step: NextStep) -> None:
        self.cmd[-1] = os.path.join(context.environment_menager.data_dir, self.cmd[-1])
        _: CompletedProcess[str] = self._run_command(self.cmd)
        next_step(context)

    @override
    def gen_command(self, *args: str, **kwargs: str) -> None:
        self.step_name.extend(f"{kwargs['in_format']}2{kwargs['out_format']}")
        self.cmd: list[str] = [
            "obabel",
            kwargs["file"],
            f"-i{kwargs['in_format']}",
            f"-o{kwargs['out_format']}",
            "-O",
            f"{self.molecule_name}.{kwargs['out_format']}",
        ]
        if kwargs["in_format"] == "smi":
            self.cmd.insert(2, "--gen3d")


class ModifyChemFile(PipeStep[MDContext]):
    def __init__(self, molecule_name: str, file: Path) -> None:
        self.molecule_name: str = molecule_name
        self.structure: pmd.Structure = self._init_structure(file)
        self.step_name: list[str] = ["MODIFY"]

    @override
    def __call__(self, context: MDContext, next_step: NextStep) -> None:
        file = context.environment_menager.data_dir / (
            self.molecule_name + self.file_ext
        )
        self.structure.save(file, overwrite=True)
        next_step(context)

    def _init_structure(self, file: Path) -> pmd.Structure:
        self.file_ext: str
        _, self.file_ext = os.path.basename(file).split(".")
        return pmd.load_file(file)

    def modify_resname(self, resname: str) -> None:
        self.step_name.extend(["RESNAME", resname])
        new_residue = pmd.Residue(resname, chain="A", number=1)
        for atom in self.structure:
            new_residue.add_atom(atom)

        reslist = pmd.ResidueList([new_residue])
        self.structure.residues = reslist


class PrepareMDOptions(PipeStep[MDContext]):
    def __init__(self) -> None:
        self.logger: logging.Logger = logging.getLogger(self.__class__.__name__)

    def __precall__(self, context: MDContext) -> None:
        """
        Prepares the MDP configuration by reading the file and converting it to a dictionary.
        """
        self.file: Path = context.CURRENT_RUN["CONFIG_FILE"]

        self.logger.info(f"Setting up MDP file: {self.file}")

    @override
    def __call__(self, context: MDContext, next_step: NextStep) -> None:
        """
        Updates the MDP configuration based on energy groups and annealing options,
        writes it back to a file, and proceeds to the next step.
        """
        self.__precall__(context)

        new_file = context.environment_menager.data_dir / self.file.name
        if new_file.exists():
            context.environment_menager.make_duplicate(new_file)

        if context.ENVIRONMENT["SOFTWARE"] == "gromacs":
            self.mdp_dict: dict[str, str] = self._parse_mdp_file(self.file)
            log_json(self.logger, "GMX MDP config options from file", self.mdp_dict)

            enrg_groups = context.ENRG_GROUPS
            self._update_mdp_with_energy_groups(enrg_groups)

            if "annealing" in self.mdp_dict:
                self._update_mdp_with_annealing(enrg_groups)

            log_json(self.logger, "Updated GMX MDP config options", self.mdp_dict)

            self._write_mdp_file(new_file, self.mdp_dict)

            self.logger.debug(f"MDP configuration saved to: {new_file}")
            next_step(context)
        elif context.ENVIRONMENT["SOFTWARE"] == "amber":
            shutil.copy2(self.file, new_file)

            self.logger.debug(f"Amber configuration saved to: {new_file}")
            next_step(context)

    def _parse_mdp_file(self, file: Path) -> dict[str, str]:
        """
        Reads an MDP file, filters out empty lines and comments, and converts it to a dictionary.
        """
        try:
            with file.open("r") as f:
                lines = f.readlines()
            filtered_lines = list(filter(self._is_valid_line, lines))
            return self._convert_lines_to_dict(filtered_lines)
        except FileNotFoundError:
            raise FileNotFoundError(f"MDP file not found: {file}")
        except Exception as e:
            raise IOError(f"Error reading MDP file: {e}")

    def _is_valid_line(self, line: str) -> bool:
        """
        Filters out empty lines and comments.
        """
        stripped_line = line.strip()
        return bool(stripped_line) and not stripped_line.startswith(";")

    def _convert_lines_to_dict(self, lines: list[str]) -> dict[str, str]:
        """
        Converts a list of MDP configuration lines into a dictionary.
        """
        try:
            return {
                key.strip(): value.strip()
                for key, value in (line.split("=", 1) for line in lines)
            }
        except ValueError as e:
            raise ValueError(f"Invalid MDP file format: {self.file}\n{e}")

    def _update_mdp_with_energy_groups(self, enrg_groups: list[str]) -> None:
        """
        Updates the MDP configuration with energy group settings.
        """
        len_groups = len(enrg_groups)
        print(enrg_groups)

        self.mdp_dict.update(
            {
                "tc-grps": " ".join(enrg_groups),
                "ref_t": f"{self.mdp_dict.get('ref_t', '')} " * len_groups,
                "tau_t": f"{self.mdp_dict.get('tau_t', '')} " * len_groups,
            }
        )
        self.logger.debug(f"Updated with energy groups: {' '.join(enrg_groups)}")

    def _update_mdp_with_annealing(self, enrg_groups: list[str]) -> None:
        """
        Updates the MDP configuration with annealing settings if present.
        """
        len_groups = len(enrg_groups)
        annealing_keys = [
            "annealing",
            "annealing-npoints",
            "annealing-time",
            "annealing-temp",
        ]
        updates = {
            key: f"{self.mdp_dict.get(key, '')} " * len_groups
            for key in annealing_keys
            if key in self.mdp_dict
        }
        self.mdp_dict.update(updates)
        self.logger.debug("Annealing options updated in MDP.")

    def _write_mdp_file(self, file_path: Path, mdp_dict: dict[str, str]) -> None:
        """
        Writes the updated MDP dictionary back to a file.
        """
        try:
            with file_path.open("w") as f:
                f.writelines(f"{key} = {value}\n" for key, value in mdp_dict.items())
        except Exception as e:
            raise IOError(f"Error writing MDP file to {file_path}: {e}")


class RunMD(PipeStep[MDContext]):
    """Run molecular dynamics simulations using Amber or GROMACS."""

    def __init__(self, number: int, rerun: bool):
        self.logger: logging.Logger = logging.getLogger(self.__class__.__name__)
        self.number: int = number
        self.rerun: bool = rerun

    def __precall__(self, context: MDContext) -> None:
        """Prepare the simulation by setting up file paths and metadata."""
        self.sim_type: str = context.CURRENT_RUN["SIM_TYPE"]
        self.config_file: Path = context.CURRENT_RUN["CONFIG_FILE"]
        self.coordinates_file: Path = context.CURRENT_RUN["START_COORDINATES_FILE"]
        self.topology_file: Path = context.CURRENT_TOPOL["FILE"]
        self.software: str = context.ENVIRONMENT["SOFTWARE"]
        self.simulation_name: str = f"{self.number}-{self.sim_type}"
        self.step_name: list[str] = ["SIMULATION", self.sim_type]
        self.mdrun_file: Path = context.environment_menager.data_dir / "md.run"
        self.resource: str = context.ENVIRONMENT["RESOURCE"]
        self.cpus_per_task: int = context.ENVIRONMENT["CPUS_PER_TASK"]
        if context.CONNECTION:
            self.ntasks: int = context.SLURM["NTASKS"]
        else:
            self.ntasks = self.cpus_per_task
        if not self.rerun:
            index = context.find_index(self.simulation_name)
            context.add_entry(index, self.simulation_name)
        self.logger.info(f"Prepared simulation: {self.simulation_name}")
        # self.logger.debug(f"Modified database: index {index}")

    @override
    def __call__(self, context: MDContext, next_step: NextStep) -> None:
        """Run the simulation and save the commands to a file."""
        self.__precall__(context)
        self._configure_cmd()
        self._to_tar()

        self._write_command_to_file()
        ShellInterface.make_executable(self.mdrun_file)

        self.logger.debug(f"Saved MDrun script to {self.mdrun_file}")
        next_step(context)

    def _configure_cmd(self) -> None:
        """Adjust the commands based on software and hardware resource settings."""
        if self.software == "amber":
            self._generate_amber_command()
            self._configure_amber_cmd()
        elif self.software == "gromacs":
            self._generate_gromacs_command()
            self._configure_gromacs_cmd()
        else:
            raise ValueError(f"Unsupported simulation software: {self.software}")

    def _generate_amber_command(self) -> None:
        """Generate the command for running Amber simulations."""
        self.cmd: list[str] = [
            "",
            "-O",
            "-i",
            self.config_file.name,
            "-p",
            self.topology_file.name,
            "-c",
            self.coordinates_file.name,
            "-r",
            f"{self.simulation_name}.rst7",
            "-x",
            f"{self.simulation_name}.nc",
            "-v",
            f"{self.simulation_name}.vel",
            "-e",
            f"{self.simulation_name}.ene",
            "-inf",
            f"{self.simulation_name}.info",
            "-l",
            f"{self.simulation_name}.mdlog",
            "-o",
            f"{self.simulation_name}.mdout",
            "\n",
        ]
        self.step_name.extend(["AMBER", str(self.number)])
        self.logger.debug("Amber command generated.")

    def _generate_gromacs_command(self) -> None:
        """Generate the command for running GROMACS simulations."""
        self.cmd = [
            "gmx",
            "grompp",
            "-f",
            self.config_file.name,
            "-p",
            self.topology_file.name,
            "-c",
            self.coordinates_file.name,
            "-o",
            f"{self.simulation_name}.tpr",
            "\n\n",
            "",
            "mdrun",
            "-deffnm",
            f"{self.simulation_name}",
        ]
        self.step_name.extend(["GROMACS", str(self.number)])
        self.logger.debug("GROMACS command generated.")

    def _to_tar(self) -> None:
        """Compress simulation results."""
        tar_command: list[str] = [
            "tar",
            "cfv",
            f"{self.simulation_name}.sim.tar",
            f"{self.simulation_name}.*",
            f"{self.config_file.name}",
            f"{self.topology_file.name}",
            f"{self.coordinates_file.name}\n",
        ]
        self.cmd.extend(tar_command)

    def _configure_amber_cmd(self) -> None:
        """Configure the Amber command based on resource settings."""
        if self.resource == "cpu":
            self.cmd[0] = f"mpirun -np {self.ntasks} pmemd.MPI"
        elif self.resource == "gpu":
            self.cmd[0] = "pmemd.cuda.MPI"

    def _configure_gromacs_cmd(self) -> None:
        """Configure the GROMACS command based on resource settings."""
        self.cmd[-4] = "gmx"
        self.cmd.extend(["-ntmpi 1 -ntomp", str(self.cpus_per_task), "\n"])

    def _write_command_to_file(self) -> None:
        """Write the generated commands to the command file."""
        with open(self.mdrun_file, "a") as run_file:
            _: int = run_file.write(" ".join(self.cmd))


class RunSLURM(PipeStep[MDContext]):
    MODULES: dict[str, dict[str, str]] = {
        "gromacs": {
            "gpu": "module load GROMACS/2021.2-fosscuda-2020b",
            "cpu": "module load GROMACS/2021-foss-2020b",
        },
        "amber": {
            "gpu": "module load Amber/22.0-foss-2021b-AmberTools-22.3-CUDA-11.4.1",
            "cpu": "module load Amber/22.0-foss-2021b-AmberTools-22.3-CUDA-11.4.1",
        },
    }

    def __init__(self) -> None:
        self.logger: logging.Logger = logging.getLogger(self.__class__.__name__)
        self.cmd: list[str] = []

    def __precall__(self, context: MDContext) -> None:
        """Prepare SLURM options from the context."""
        slurm = context.SLURM
        env = context.ENVIRONMENT

        self.nodes: int = slurm.get("NODES", 1)
        self.cpus_per_task: int = env["CPUS_PER_TASK"]
        self.ntasks: int = slurm.get("NTASKS", 1)
        self.memory: str = slurm.get("MEMORY", "4GB")
        self.time: str = slurm.get("TIME", "1:00:00")
        self.job_name: str = env.get("BASENAME", "job")
        self.partition: str = slurm.get("PARTITION", "Tesla")
        self.account: str = slurm.get("ACCOUNT", "default")
        self.software: str = env.get("SOFTWARE", "unknown")
        self.resource: str = slurm.get("RESOURCE", "cpu")
        self.data_dir: Path = context.environment_menager.data_dir
        self.project_name: str = env["PROJECT_NAME"]
        self.remote_dir: Path = slurm.get("REMOTE_DIR", "/home/mszatko/MD")
        self.lustre_dir: Path = slurm["LUSTRE_DIR"]

        self.qos: str | None = slurm.get("QOS")
        self.ngpu: int | None = slurm.get("NGPU")
        gpu_resources: str | None = slurm.get("GPU_RESOURCES")
        self.gpu_resources: str = f"gpu:{gpu_resources}:{self.ngpu}"

        self.logger.info("SLURM configuration initialized.")

    @override
    def __call__(self, context: MDContext, next_step: NextStep) -> None:
        """Generate and save the SLURM batch file."""
        self.__precall__(context)
        self._configure_cmd()
        file_path = self.data_dir / "md.slurm"

        if Path(file_path).exists():
            context.environment_menager.make_duplicate(file_path)
        self._write_slurm_file(file_path)
        ShellInterface.make_executable(file_path)

        self.logger.info(f"SLURM batch file created at: {file_path}")
        next_step(context)

    def _configure_cmd(self) -> None:
        """Generate the SLURM command based on the context."""
        self.cmd = [
            f"{self._slurm_options()}\n",
            "source /usr/local/sbin/modules.sh\n",
            f"""if  [ !  -d  {self.lustre_dir / self.project_name} ]; then
    mkdir -p {self.lustre_dir / self.project_name}
fi
""",
            f"{self._get_software_module()}\n",
            f"cp {self.remote_dir}/{self.project_name}/* $TMPDIR/ -rf\n",
            "echo $TMPDIR\n",
            "cd $TMPDIR\n",
            *self._extract_mdrun(),
        ]

    def _slurm_options(self) -> str:
        """Generate SLURM batch options."""
        slurm_script = f"""#!/bin/bash
#SBATCH --nodes={self.nodes}
#SBATCH --cpus-per-task={self.cpus_per_task}
#SBATCH --ntasks={self.ntasks}
#SBATCH --mem={self.memory}
#SBATCH --time={self.time}
#SBATCH --job-name={self.job_name}
#SBATCH --account={self.account}
"""
        if self.resource == "gpu":
            slurm_script += (
                f"#SBATCH --partition={self.partition}\n"
                f"#SBATCH --qos={self.qos}\n"
                f"#SBATCH --gres={self.gpu_resources}\n"
            )
            self.logger.debug("Added GPU-specific SLURM options.")
        return slurm_script

    def _get_software_module(self) -> str:
        """Determine the module command for the specified software and resource."""
        software_modules = self.MODULES.get(self.software)
        if not software_modules:
            self.logger.error(f"Unsupported software: {self.software}")
            raise ValueError(f"Unsupported software: {self.software}")
        module_command = software_modules.get(self.resource)
        if not module_command:
            self.logger.error(f"Unsupported resource type: {self.resource}")
            raise ValueError(f"Unsupported resource type: {self.resource}")
        return module_command

    def _write_slurm_file(self, file_path: Path) -> None:
        """Write the SLURM script to a file."""
        try:
            with open(file_path, "w") as run_file:
                run_file.writelines(self.cmd)
            self.logger.debug(f"SLURM script saved to: {file_path}")
        except Exception as e:
            self.logger.error(f"Failed to write SLURM script: {e}")
            raise

    def _extract_mdrun(self) -> list[str]:
        """Extract md.run bash script into a slurm script."""
        with open(self.data_dir / "md.run", "r") as md_run_file:
            md_run_commands = md_run_file.readlines()

        log_ext: str = ""
        if self.software == "amber":
            log_ext = "mdout"
        if self.software == "gromacs":
            log_ext = "log"

        if log_ext == "":
            raise ValueError("Software has not been recognized.")

        slurm_script: list[str] = []
        for line in md_run_commands:
            slurm_script.append(line)
            stripped_line = line.strip()

            if stripped_line.startswith("tar"):
                try:
                    simulation_name = stripped_line.split()[2].split(".")[0]
                except IndexError:
                    raise ValueError(f"Malformed tar command: {stripped_line}")

                slurm_script.extend(
                    [
                        f"mv -v {simulation_name}.sim.tar {self.lustre_dir}/{self.project_name}/\n",
                        f"cp -v {simulation_name}.{log_ext} {self.remote_dir}/{self.project_name}/\n",
                    ]
                )

        return slurm_script


class CheckProgress(PipeStep[MDContext]):
    job_name: str
    extension: str
    SUPPORTED_SOFTWARE: dict[str, dict[str, str]] = {
        "gromacs": {
            "extension": ".log",
            "nsteps_key": "nsteps",
            "steps_key": "Statistics",
        },
        "amber": {
            "extension": ".mdout",
            "nsteps_key": "nstlim",
            "steps_key": "NSTEP",
        },
    }

    def __init__(self, log_file: Path) -> None:
        self.logger: logging.Logger = logging.getLogger(self.__class__.__name__)
        self.log_content: list[str] = self._read_log(log_file)
        self.job_name, self.extension = self._init_job_name(log_file)

    def __precall__(self, context: MDContext) -> None:
        self.logger.info(
            f"Initializing CheckProgress for file: {self.job_name + self.extension}"
        )

        self.software: str = self._init_software()
        self.nsteps: int = self._extract_nsteps()

        self.query: dict[str, str] = {
            "PROJECT NAME": context.ENVIRONMENT["PROJECT_NAME"],
            "SIMULATION NAME": self.job_name,
        }

    @override
    def __call__(self, context: MDContext, next_step: NextStep) -> None:
        self.__precall__(context)

        finished = False
        database_entry = context.database_menager.find_entries(self.query)
        if database_entry["STAGE"].tolist()[0] in ["Finished", "DOWNLOADED"]:
            self.logger.info(
                f"{context.ENVIRONMENT['PROJECT_NAME']} - {self.job_name} is already marked as finished in the database."
            )
            finished = True

        if not finished:
            # Count completed steps
            done_steps = self._count_steps()
            if done_steps == self.nsteps:
                modify_query = ("STAGE", "Finished")
                self.logger.info(
                    f"{context.ENVIRONMENT['PROJECT_NAME']} - {self.job_name} is finished. Total steps completed: {done_steps}"
                )
            else:
                modify_query = ("STAGE", "Unfinished")
                self.logger.warning(
                    f"{context.ENVIRONMENT['PROJECT_NAME']} is incomplete. Steps completed: {done_steps}"
                )

            # Modify and save the database
            context.database_menager.modify_entry(modify_query, self.query)
            context.database_menager.database.save()
            self.logger.debug("Database updated and saved.")
        next_step(context)

    def _init_job_name(self, log_file: Path) -> tuple[str, str]:
        """Extract the job name and file extension."""
        return os.path.splitext(log_file.name)

    def _read_log(self, log_file: Path) -> list[str]:
        """Read the log file and return its content as a list of lines."""
        try:
            with open(log_file, "r") as file:
                return file.readlines()
        except FileNotFoundError:
            self.logger.error(f"Log file not found: {log_file}")
            raise
        except Exception as e:
            self.logger.error(f"Error reading log file {log_file}: {e}")
            raise

    def _init_software(self) -> str:
        """Determine the software type based on the file extension."""
        for software, config in self.SUPPORTED_SOFTWARE.items():
            if self.extension == config["extension"]:
                return software
        self.logger.error(f"Unsupported file extension: {self.extension}")
        raise ValueError(f"Unknown software for extension {self.extension}")

    def _extract_nsteps(self) -> int:
        """Extract the total number of steps from the log file."""
        nsteps_key = self.SUPPORTED_SOFTWARE[self.software]["nsteps_key"]
        for line in self.log_content:
            if nsteps_key in line:
                match = re.search(r"\d+", line)
                if match:
                    return int(match.group())
        self.logger.error("Failed to find nsteps information in the log file.")
        raise ValueError("Corrupted or incomplete log file.")

    def _count_steps(self) -> int:
        """Count the number of completed steps."""
        steps_key = self.SUPPORTED_SOFTWARE[self.software]["steps_key"]
        steps_done: int = -1
        for line in self.log_content:
            if steps_key in line.strip().split():
                match = re.search(r"\d+", line)
            else:
                continue
            if match:
                steps_done = int(match.group())
                if self.software == "gromacs":  # Adjust for GROMACS
                    steps_done -= 1
        if steps_done < 0:
            self.logger.error("Failed to count steps from the log file.")
            raise ValueError("Corrupted or incomplete log file.")
        else:
            return steps_done


if __name__ == "__main__":
    # gmx_file = pmd.gromacs.GromacsTopologyFile("test/a1.top")
    # gmx_gro = pmd.gromacs.GromacsGroFile.parse("test/10ns_a1-1.gro")
    # gmx_file.positions = gmx_gro.positions
    # unk_file = gmx_file[":UNK"]
    # unk_file.save("test/a1.pdb", overwrite=True)

    pdb_file: pmd.Structure = pmd.load_file("test/chcl3.top")
    print(pdb_file.residues)
    print(pdb_file.atoms)
    # pdb_file[1].residue = pmd.Residue("MKRM", chain="A", number=1)
    # print(pdb_file[1].residue)

    # for atom in pdb_file:
    #     atom.residue = pmd.Residue("MKRM", chain="A", number=1)

    # print(pdb_file.atoms)
    # print(pdb_file[0].residue)
    # print(pdb_file.residues)

    # for index, _ in enumerate(pdb_file.residues):
    #     pdb_file.residues[index] = pmd.Residue("MKRM", chain="A", number=1)

    mkrm_resname = pmd.Residue("MKR", chain="A", number=1)

    for atom in pdb_file:
        mkrm_resname.add_atom(atom)

    reslist = pmd.ResidueList([mkrm_resname])
    # print(str(reslist))
    pdb_file.residues = reslist
    # print(pdb_file.atoms)
    # print(pdb_file[0].residue)
    print(pdb_file.residues[0].name)
    pdb_file.save("test/good_resname.pdb", overwrite=True)
