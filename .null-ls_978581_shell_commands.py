import logging
import os
import re
from pathlib import Path
from typing import Any

import parmed as pmd

from context import MDContext
from interfaces import PipeStepInterface, ShellInterface, verbose_call
from pipeline import NextStep


class ObabelShell(ShellInterface):
    def __init__(self, molecule_name: str) -> None:
        self.molecule_name = molecule_name
        self.step_name = ["OBABELRUN", self.molecule_name]

    @verbose_call
    def __call__(self, context: MDContext, next_step: NextStep) -> None:
        self.cmd[-1] = os.path.join(context.DATA_DIR, self.cmd[-1])
        self._run_command(self.cmd)
        next_step(context)

    def gen_command(self, file: str, in_format: str, out_format: str):
        self.step_name.extend(f"{in_format}2{out_format}")
        self.cmd = [
            "obabel",
            file,
            f"-i{in_format}",
            f"-o{out_format}",
            "-O",
            f"{self.molecule_name}.{out_format}",
        ]
        if in_format == "smi":
            self.cmd.insert(2, "--gen3d")


class ModifyChemFile(PipeStepInterface):
    def __init__(self, molecule_name: str, file: str) -> None:
        self.molecule_name = molecule_name
        self.structure = self._init_structure(file)
        self.step_name = ["MODIFY"]

    @verbose_call
    def __call__(self, context: MDContext, next_step: NextStep) -> None:
        file = context.PATHS_DATA_DIR / (self.molecule_name + self.file_ext)
        self.structure.save(file, overwrite=True)
        next_step(context)

    def _init_structure(self, file: str) -> pmd.Structure:
        _, self.file_ext = os.path.basename(file).split(".")
        return pmd.load_file(file)

    def modify_resname(self, resname: str) -> None:
        self.step_name.extend(["RESNAME", resname])
        new_residue = pmd.Residue(resname, chain="A", number=1)
        for atom in self.structure:
            new_residue.add_atom(atom)

        reslist = pmd.ResidueList([new_residue])
        self.structure.residues = reslist


class RunMD(ShellInterface):
    sim_type: str
    software: str
    number: int

    file: Path
    topology_file: Path
    positions_file: Path

    def __init__(self, **kwargs: Any):
        self.logger = logging.getLogger(self.__class__.__name__)

        self.__dict__.update(kwargs)
        self.job_name = f"{self.number}-{self.sim_type}"
        self.step_name = ["SIMULATION", self.sim_type]

        self.logger.info(f"Running simulation {str(self.job_name)}")

    def __call__(self, context: MDContext, next_step: NextStep) -> None:
        file_path = context.PATHS_DATA_DIR / "md.run"

        context.CURRENT_TOPFILE = self.topology_file
        context.CURRENT_POSFILE = self.positions_file
        context.CURRENT_CONFIGFILE = self.file

        index = context.find_index(self.job_name)
        context.add_entry(index, f"{self.job_name}")

        self.logger.debug(f"Modified database: index {index}")

        if self.software == "gromacs":
            self.cmd[-4] = "gmx"
            self.cmd.extend(["-nt", str(context.SLURM_CPUS_PER_TASK), "\n"])
            # if context.SLURM_RESOURCE == "cpu":
            #     self.cmd[-4] = "gmx"
            #     self.cmd.extend(
            #         ["-nt", str(context.SLURM_CPUS_PER_TASK), "\n"])
            # if context.SLURM_RESOURCE == "gpu":
            #     self.cmd[-4] = "gmx"
            #     self.cmd.extend(
            #         ["-nt", str(context.SLURM_CPUS_PER_TASK), "\n"])
        if self.software == "amber":
            if context.SLURM_RESOURCE == "cpu":
                self.cmd[0] = f"mpirun -np {context.SLURM_NTASKS} pmemd.MPI"
            if context.SLURM_RESOURCE == "gpu":
                self.cmd[0] = "pmemd.cuda.MPI"

        with open(file_path, "a") as run_file:
            msg = " ".join(self.cmd)
            run_file.writelines(msg)
        self._make_executable(file_path)

        self.logger.debug(f"Saved MDrun script {str(file_path)}")
        next_step(context)

    def gen_command(self) -> None:
        if self.software == "amber":
            self.cmd = [
                "",
                "-O",
                "-i",
                self.file.name,  # input md options, 3
                "-p",
                self.topology_file.name,  # parameter file, 5, .parm7
                "-c",
                self.positions_file.name,  # coordinate file , 7, .rst7
                "-r",
                f"{self.job_name}.rst7",
                "-x",
                f"{self.job_name}.nc",
                "-v",
                f"{self.job_name}.vel",
                "-e",
                f"{self.job_name}.ene",
                "-inf",
                f"{self.job_name}.info",
                "-l",
                f"{self.job_name}.mdlog",
                "-o",
                f"{self.job_name}.mdout",
                "\n",
            ]
            self.step_name.extend(["AMBER", str(self.number)])
            self.logger.debug("Setting amber run")
        if self.software == "gromacs":
            self.cmd = [
                "gmx",
                "grompp",
                "-f",
                self.file.name,
                "-p",
                self.topology_file.name,  # parameter file, 5, .top
                "-c",
                self.positions_file.name,  # coordinate file, 7, .gro
                "-o",
                f"{self.job_name}.tpr",
                "\n\n",
                "",
                "mdrun",
                "-deffnm",
                f"{self.job_name}",
            ]
            self.step_name.extend(["GROMACS", str(self.number)])
            self.logger.debug("Setting gromacs run")


class RunSLURM(ShellInterface):
    nodes: int
    cpus_per_task: int
    ntasks: int
    memory: str
    time: str
    job_name: str
    partition: str = "tesla"  # A100 or Tesla
    qos: str = "tesla"  # a100 or tesla
    account: str = "kde"
    gpu_resources: str = "tesla"  # name of the node
    ngpu: int = 1

    software: str
    resource: str

    source_module = "source /usr/local/sbin/modules.sh"
    gromacs_gpu = "module load GROMACS/2021.2-fosscuda-2020b"
    gromacs_cpu = "module load GROMACS/2021-foss-2020b"
    amber_gpu = "module load Amber/22.0-foss-2021b-AmberTools-22.3-CUDA-11.4.1"
    amber_cpu = "module load Amber/22.0-foss-2021b-AmberTools-22.3-CUDA-11.4.1"

    def __init__(self, **kwargs: Any) -> None:
        self.logger = logging.getLogger(self.__class__.__name__)

        for kwarg in kwargs:
            self.__dict__.update(kwargs)
        self.gpu_resources = f"gpu:{self.gpu_resources}:{self.ngpu}"

        self.logger.info("Constructing SLURM file")

    def __call__(self, context: MDContext, next_step: NextStep) -> None:
        self.cmd.extend([f"cd {context.PATHS_REMOTE_DIR}\n", "./md.run\n"])
        file_path = context.PATHS_DATA_DIR / "md.slurm"
        with open(file_path, "w") as run_file:
            msg = "\n".join(self.cmd)
            run_file.writelines(msg)
        self._make_executable(file_path)

        self.logger.debug(f"Saved to {str(file_path)}")
        next_step(context)

    def _slurm_options(self) -> str:
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
            slurm_script += f"#SBATCH --partition={self.partition}\n"
            slurm_script += f"#SBATCH --qos={self.qos}\n"
            slurm_script += f"#SBATCH --gres={self.gpu_resources}\n"
            self.logger.debug("Added gpu options")
        return slurm_script

    def _hardware_options(self) -> str:
        if self.software == "gromacs":
            if self.resource == "gpu":
                return self.gromacs_gpu
            if self.resource == "cpu":
                return self.gromacs_cpu
        if self.software == "amber":
            if self.resource == "gpu":
                return self.amber_gpu
            if self.resource == "cpu":
                return self.amber_cpu
        self.logger.debug(f"Hardware options: {self.software}, {self.resource}")
        return ""

    def gen_command(self) -> None:
        self.cmd = [
            self._slurm_options(),
            self.source_module,
            self._hardware_options(),
        ]


class CheckProgerss(PipeStepInterface):
    def __init__(self, log_file: Path) -> None:
        self.logger = logging.getLogger(self.__class__.__name__)
        self.logger.info(f"Checking file {str(log_file)}")

        self.log_content = self._read_log(log_file)
        self.job_name, self.extention = self._init_job_name(log_file)
        self.software = self._init_software()
        self.nsteps = self._init_nsteps()

    def __call__(self, context: MDContext, next_step: NextStep) -> None:
        job_kwargs = {
            "PROJECT NAME": context.TITLE_PROJECT_NAME,
            "SIMULATION NAME": self.job_name,
        }

        database_entry = context.DATABASE.find_entries(**job_kwargs)
        if database_entry["STAGE"].tolist() == ["Finished"]:
            self.logger.info("Job has been finished already.")
            next_step(context)

        done_steps = self.count_steps()
        if self.nsteps == done_steps:
            stage_dict = {"STAGE": "Finished"}
            self.logger.info("Job has been finished.")
            self.logger.debug(f"Steps done {done_steps}")
        else:
            stage_dict = {"STAGE": "Unfinished"}
            self.logger.info("Job was not finished.")
            self.logger.debug(f"Steps done {done_steps}")

        context.DATABASE.modify(
            stage_dict,
            **job_kwargs,
        )
        context.DATABASE.save()
        self.logger.debug("Modified and save database")
        next_step(context)

    def _init_job_name(self, log_file: Path) -> tuple[Any, Any]:
        basename = os.path.basename(log_file)
        return os.path.splitext(basename)

    def _read_log(self, log_file: Path):
        with open(log_file, "r") as file:
            return file.readlines()

    def _init_software(self) -> str:
        if self.extention == ".log":
            return "gromacs"
        if self.extention == ".mdout":
            return "amber"
        print("Could not guess what type of software is this from.")
        exit(1)

    def _init_nsteps(self) -> int:
        if self.software == "gromacs":
            option = "nsteps"
        if self.software == "amber":
            option = "nstlim"
        for line in self.log_content:
            if option in line:
                nsteps = int(re.findall(r"\d+", line)[0])
                break
        return nsteps

    def count_steps(self) -> int:
        if self.software == "gromacs":
            option = "Statistics"
        if self.software == "amber":
            option = "NSTEP"
        steps_done = 0
        for line in self.log_content:
            if option in line:
                steps_done = int(re.findall(r"\d+", line)[0])
                if self.software == "gromacs":
                    steps_done -= 1
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
