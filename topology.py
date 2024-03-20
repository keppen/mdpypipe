import logging
import os
from pathlib import Path
from typing import Any, Dict, List

import parmed as pmd
from numpy.typing import ArrayLike

from context import ContextMD
from interfaces import (
    PipeStepInterface,
    ShellInterface,
    TopologyReadInterface,
    verbose_call,
)
from logger import log_json
from pipeline import NextStep


class ReadTopology(TopologyReadInterface):
    def __init__(self, name: str, file: Path, ff: str, times: int = 1) -> None:
        self.logger = logging.getLogger(self.__class__.__name__)

        self.logger.info(f"Reading topology file {str(file)}")

        self.name = name
        self.step_name = ["LOAD_TOPOLOGY", self.name]
        self.software = self._check_extention(file)
        self.structure = self.read_topology(file, ff)
        self.structure *= times

    @verbose_call
    def __call__(self, context: ContextMD, next_step: NextStep) -> None:
        context.TOPOLOGIES[self.name] = self.structure

        self.logger.debug("Structure loaded: " + str(self.structure))
        next_step(context)

    def read_topology(self, file: Path, ff: str) -> pmd.Structure:
        self.step_name.extend([str(file), ff])
        if self.software == "amber":
            structure = pmd.amber.AmberFormat(str(file))
        if self.software == "gromacs":
            structure = pmd.gromacs.GromacsTopologyFile(str(file))
        else:
            raise FileNotFoundError
        return self._reduce(self._change_type(structure))

    def _reduce(self, structure: pmd.Structure):
        return structure.split()[0][0]

    def _change_type(self, structure: pmd.Structure) -> pmd.Structure:
        return structure.copy(pmd.Structure)


class ReadPositions(TopologyReadInterface):
    def __init__(self, file: Path) -> None:
        self.logger = logging.getLogger(self.__class__.__name__)
        self.logger.info(f"Reading positons file {str(file)}")

        self.positions_data = self.read_positions(file)
        self.step_name = ["LOAD_POSITIONS", str(file)]

    @verbose_call
    def __call__(self, context: ContextMD, next_step: NextStep):
        context.POSITIONS = self.positions_data

        self.logger.debug("Loaded positions")
        next_step(context)

    def read_positions(self, file: Path) -> pmd.unit.Quantity:
        software = self._check_extention(file)
        if software == "amber":
            return pmd.amber.Rst7(str(file)).positions
        if software == "gromacs":
            return pmd.gromacs.GromacsGroFile.parse(str(file)).positions


class ReadBox(TopologyReadInterface):
    def __init__(self, file: Path) -> None:
        self.logger = logging.getLogger(self.__class__.__name__)
        self.logger.info(f"Reading positons file {str(file)}")

        self.box = self.read_box(file)
        self.step_name = ["LOAD_BOX", str(file)]

    @verbose_call
    def __call__(self, context: ContextMD, next_step: NextStep):
        context.BOX = self.box

        self.logger.debug(f"Loaded bos {str(self.box)}")
        next_step(context)

    def read_box(self, file: Path) -> ArrayLike:
        software = self._check_extention(file)
        if software == "amber":
            return pmd.amber.AmberFormat(str(file)).box
        if software == "gromacs":
            return pmd.gromacs.GromacsGroFile.parse(str(file)).box


class WriteParameters(TopologyReadInterface):
    def __init__(self, basename: str, software: str) -> None:
        self.logger = logging.getLogger(self.__class__.__name__)
        self.logger.info("Writing paramters")
        self.software = software
        self.basename = basename
        self.step_name = ["WRITTING_PARAMS", self.basename]
        self.ext = self._init_extention()

    @verbose_call
    def __call__(self, context: ContextMD, next_step: NextStep) -> None:
        topology_file = context.PATHS_DATA_DIR / (self.basename + self.ext)
        context.STRUCTURE.save(
            str(topology_file),
            overwrite=True,
        )
        context.CURRENT_TOPFILE = topology_file

        self.logger.debug(
            f"Writing paramters to file {self.basename + self.ext}")
        next_step(context)

    def _init_extention(self) -> str:
        if self.software == "amber":
            return ".parm7"

        if self.software == "gromacs":
            return ".top"
        return ""


class WritePositions(TopologyReadInterface):
    def __init__(self, basename: str, software: str) -> None:
        self.logger = logging.getLogger(self.__class__.__name__)
        self.logger.info("Writing positions")

        self.software = software
        self.basename = basename
        self.step_name = ["WRITTING_COORDS", self.basename]
        self.ext = self._init_extention()

    @verbose_call
    def __call__(self, context: ContextMD, next_step: NextStep) -> None:
        positions_file = context.PATHS_DATA_DIR / (self.basename + self.ext)
        context.STRUCTURE.save(
            str(positions_file),
            overwrite=True,
        )
        context.CURRENT_POSFILE = positions_file

        self.logger.debug(
            f"Writing positions to file {self.basename + self.ext}")
        next_step(context)

    def _init_extention(self) -> str:
        if self.software == "amber":
            return ".rst7"

        if self.software == "gromacs":
            return ".gro"
        return ""


class PrepareMDP(PipeStepInterface):
    def __init__(self, file: Path) -> None:
        self.logger = logging.getLogger(self.__class__.__name__)
        self.logger.info(f"Setting up {str(file)}")

        self.file_name = file.name
        self.mdp_dict = self.to_dict(self._read_file(file))

        log_json(self.logger, "GMX MDP config options from file", self.mdp_dict)

    def __call__(self, context: ContextMD, next_step: NextStep) -> None:
        enrg_groups = context.ENRG_GROUPS
        len_enrg_groups = len(enrg_groups)

        update_mdp = {
            "tc-grps": " ".join(enrg_groups),
            "ref_t": f"{self.mdp_dict['ref_t'] } " * len_enrg_groups,
            "tau_t": f"{self.mdp_dict['tau_t'] } " * len_enrg_groups,
        }
        self.mdp_dict.update(update_mdp)
        self.logger.debug(f"Found {' '.join(enrg_groups)}")

        if "annealing" in self.mdp_dict.keys():
            update_mdp = {
                "annealing": f"{self.mdp_dict['annealing'] } " * len_enrg_groups,
                "annealing-npoints": f"{self.mdp_dict['annealing-npoints'] } "
                * len_enrg_groups,
                "annealing-time": f"{self.mdp_dict['annealing-time'] } "
                * len_enrg_groups,
                "annealing-temp": f"{self.mdp_dict['annealing-temp'] } "
                * len_enrg_groups,
            }
            self.mdp_dict.update(update_mdp)
            self.logger.debug("MD options had annealing")

        log_json(self.logger, "New GMX MDP config options", self.mdp_dict)

        file_path = context.PATHS_DATA_DIR / self.file_name
        with open(file_path, "w") as mdp_file:
            msg = "\n".join(self.to_list(self.mdp_dict))
            mdp_file.writelines(msg)

        self.logger.debug(f"Saved to file {str(file_path)}")
        next_step(context)

    def _read_file(self, file: Path) -> List[str]:
        with open(file, "r") as file_content:
            lines = file_content.readlines()
            lines = list(filter(PrepareMDP.fileter_empty, lines))
            return list(filter(PrepareMDP.filter_comments, lines))

    def to_dict(self, lines: List[str]) -> Dict[str, str]:
        splitted_lines = [i.split("=") for i in lines]
        return {k.strip(): v.strip() for k, v in splitted_lines}

    def to_list(self, options_dict: Dict[str, str]) -> List[str]:
        return [f"{k} = {v}" for k, v in options_dict.items()]

    @staticmethod
    def fileter_empty(line: str) -> bool:
        if len(line.strip()) == 0:
            return False
        return True

    @staticmethod
    def filter_comments(line: str) -> bool:
        if line.strip().startswith(";"):
            return False
        return True


if __name__ == "__main__":
    import context as cnx
    import pipeline as pip
    from shell_commands import RunMD

    def chk_instance(a, b):
        print(f"Check if {a} is instance of {b}: {isinstance(a, b)}")

    # chk_instance(intf.PipeStepInterface, pip.PipeStep)
    # a = ReadCoordinates("aaa")
    # b = ReadTopology("AAA")
    # c = cnx.ContextMD("a")
    # chk_instance(a, pip.PipeStep)
    # chk_instance(a, intf.PipeStepInterface)
    # chk_instance(b, pip.PipeStep)
    # chk_instance(b, intf.PipeStepInterface)
    # chk_instance(c, pip.PipeStep)
    # chk_instance(c, intf.PipeStepInterface)
    # print(a == b)

    MKR_name = "A1"
    SOL_name = "CHCL3"
    basename = f"{MKR_name}in{SOL_name}"
    read_topology_MRK_dict = {"file": "test/a1_test.top", "ff": "Amber14SB"}
    read_topology_SOL_dict = {"file": "test/chcl3.top", "ff": "GAFF"}
    read_positions_dict = {"file": "test/10ns_a1-1.gro"}
    runMD_dict = {
        "file": "/home/keppen/MD/data/amber-md/classic.md",
        "software": "amber",
        "number": 0,
    }

    # context = cnx.ContextMD(basename)
    # for arg_dict in [
    #     read_positions_dict,
    #     read_topology_SOL_dict,
    #     read_topology_MRK_dict,
    #     runMD_dict,
    # ]:
    #     context.copy_init_files(arg_dict)
    # job1 = ReadTopology(MKR_name)
    # job1.read_topology(**read_topology_MRK_dict)
    # job2 = ReadTopology(SOL_name)
    # job2.read_topology(**read_topology_SOL_dict)
    # job3 = ReadPositions(basename)
    # job3.read_positions(**read_positions_dict)
    # job8 = ReadBox(basename)
    # job8.read_box(**read_positions_dict)
    # job4 = WriteParameters(basename, "gromacs")
    # job5 = WritePositions(basename, "gromacs")
    # job6 = WriteParameters(basename, "amber")
    # job7 = WritePositions(basename, "amber")
    # job9 = RunMD(basename, "10ns")
    # job9.gen_command(**runMD_dict)
    # pipe: pip.Pipeline = pip.Pipeline(job1, job2, job3, job8, job6, job7, job9)
    # pipe(context)
