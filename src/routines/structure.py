import logging
from pathlib import Path
from typing import override

import parmed as pmd
from numpy.typing import ArrayLike

from src.context.context import MDContext
from src.interfaces.interfaces import TopologyReadInterface
from src.interfaces.pipeline import NextStep


class ReadTopology(TopologyReadInterface):
    def __init__(self) -> None:
        self.logger: logging.Logger = logging.getLogger(self.__class__.__name__)

    def __precall__(self, context: MDContext) -> None:
        self.name: str = context.CURRENT_TOPOL["NAME"]
        self.file: Path = context.CURRENT_TOPOL["FILE"]
        self.ff: str = context.CURRENT_TOPOL["FF"]
        self.software: str = self._check_extension(self.file)

        self.logger.info(f"Reading topology file {str(self.file)}")

    @override
    def __call__(self, context: MDContext, next_step: NextStep) -> None:
        self.__precall__(context)
        self.structure: pmd.Structure = self._read_topology()
        self.structure *= context.CURRENT_TOPOL["NUMBER"]
        context.CURRENT_TOPOL["TOPOLOGY"] = self.structure

        self.logger.debug("Structure loaded: " + str(self.structure))
        next_step(context)

    def _read_topology(self) -> pmd.Structure:
        if self.software == "amber":
            topology = pmd.amber.AmberFormat.parse(str(self.file))
        elif self.software == "gromacs":
            topology = pmd.gromacs.GromacsTopologyFile(str(self.file))
        else:
            raise ValueError(f"Unsupported simulation software: {self.software}")
        return self._reduce(self._change_type(topology))

    def _reduce(self, structure: pmd.Structure) -> pmd.Structure:
        return structure.split()[0][0]

    def _change_type(self, structure: pmd.Structure) -> pmd.Structure:
        return structure.copy(pmd.Structure)


class ReadCoordinates(TopologyReadInterface):
    def __init__(self) -> None:
        self.logger: logging.Logger = logging.getLogger(self.__class__.__name__)

    def __precall__(self, context: MDContext) -> None:
        self.file: Path = context.CURRENT_RUN["START_COORDINATES_FILE"]
        self.software: str = self._check_extension(self.file)

        self.logger.info(f"Reading positions file {self.file}")

    @override
    def __call__(self, context: MDContext, next_step: NextStep) -> None:
        self.__precall__(context)

        self.coordinates: pmd.unit.Quantity = self._read_coordinates()
        context.CURRENT_RUN["START_COORDINATES"] = self.coordinates

        self.box: ArrayLike = self._read_box()
        context.CURRENT_RUN["START_BOX"] = self.box

        self.logger.debug(f"Loaded box {str(self.box)}")
        self.logger.debug("Loaded positions")
        next_step(context)

    def _read_coordinates(self) -> pmd.unit.Quantity:
        """Read positions from the file based on the simulation software."""

        if self.software == "amber":
            coordinates = pmd.amber.Rst7(str(self.file))
        elif self.software == "gromacs":
            coordinates = pmd.gromacs.GromacsGroFile.parse(str(self.file))
        else:
            raise ValueError(f"Unsupported simulation software for file: {self.file}")

        if coordinates.positions is None:
            raise ValueError(f"Positions data is missing in the file: {self.file}")

        return coordinates.positions

    def _read_box(self) -> ArrayLike:
        """Read box from the file based on the simulation software."""

        if self.software == "amber":
            box = pmd.amber.Rst7(str(self.file))
        elif self.software == "gromacs":
            box = pmd.gromacs.GromacsGroFile.parse(str(self.file))
        else:
            raise ValueError(f"Unsupported simulation software for file: {self.file}")

        if box.box is None:
            raise ValueError(f"box data is missing in the file: {self.file}")

        return box.box


# class ReadBox(TopologyReadInterface):
#     def __init__(self) -> None:
#         self.logger: logging.Logger = logging.getLogger(self.__class__.__name__)
#
#     def __precall__(self, context: MDContext) -> None:
#         self.file: Path = context.CURRENT_RUN["START_BOX_FILE"]
#         self.software: str = self._check_extension(self.file)
#
#         self.step_name: list[str] = ["LOAD_BOX", str(self.file)]
#         self.logger.info(f"Reading positons file {str(self.file)}")
#
#     @override
#     def __call__(self, context: MDContext, next_step: NextStep):
#         self.__precall__(context)
#         self.box: ArrayLike = self._read_box()
#         context.CURRENT_RUN["START_BOX"] = self.box
#
#         self.logger.debug(f"Loaded bos {str(self.box)}")
#         next_step(context)


class WriteParameters(TopologyReadInterface):
    def __init__(self) -> None:
        self.logger: logging.Logger = logging.getLogger(self.__class__.__name__)

    def __precall__(self, context: MDContext) -> None:
        self.software: str = context.ENVIRONMENT["SOFTWARE"]
        self.basename: str = context.ENVIRONMENT["BASENAME"]
        self.extension: str = self._file_extention()
        self.new_topology_file: Path = context.environment_menager.data_dir / (
            self.basename + self.extension
        )
        if self.new_topology_file.exists():
            context.environment_menager.make_duplicate(self.new_topology_file)

        self.logger.info("Writing paramters.")

    @override
    def __call__(self, context: MDContext, next_step: NextStep) -> None:
        self.__precall__(context)
        try:
            context.STRUCTURE.save(
                str(self.new_topology_file),
                overwrite=True,
            )
            context.CURRENT_TOPOL["FILE"] = self.new_topology_file

            self.logger.debug(f"Writing paramters to file {self.new_topology_file}")
            next_step(context)
        except Exception as e:
            raise IOError(
                f"Error writing parameters to file {self.new_topology_file}: {e}"
            )

    def _file_extention(self) -> str:
        if self.software == "amber":
            return ".parm7"

        if self.software == "gromacs":
            return ".top"
        raise ValueError(f"Unsupported simulation software: {self.software}")


class ReadRawTopology(TopologyReadInterface):
    def __init__(self) -> None:
        self.logger: logging.Logger = logging.getLogger(self.__class__.__name__)

    def __precall__(self, context: MDContext) -> None:
        self.name: str = context.CURRENT_TOPOL["NAME"]
        self.file: Path = context.CURRENT_TOPOL["FILE"]
        self.ff: str = context.CURRENT_TOPOL["FF"]
        self.software: str = self._check_extension(self.file)

        self.logger.info(f"Reading topology file {str(self.file)}")

    @override
    def __call__(self, context: MDContext, next_step: NextStep) -> None:
        self.__precall__(context)
        self.structure: pmd.Structure = self._read_topology()
        self.structure *= context.CURRENT_TOPOL["NUMBER"]
        context.CURRENT_TOPOL["TOPOLOGY"] = self.structure

        self.logger.debug("Structure loaded: " + str(self.structure))
        next_step(context)

    def _read_topology(self) -> pmd.Structure:
        if self.software == "amber":
            topology = pmd.amber.AmberFormat.parse(str(self.file))
        elif self.software == "gromacs":
            topology = pmd.gromacs.GromacsTopologyFile(str(self.file))
        else:
            raise ValueError(f"Unsupported simulation software: {self.software}")
        return self._reduce(self._change_type(topology))

    def _reduce(self, structure: pmd.Structure) -> pmd.Structure:
        return structure.split()[0][0]

    def _change_type(self, structure: pmd.Structure) -> pmd.Structure:
        return structure.copy(pmd.Structure)


class WriteRawParameters(TopologyReadInterface):
    def __init__(self) -> None:
        self.logger: logging.Logger = logging.getLogger(self.__class__.__name__)

    def __precall__(self, context: MDContext) -> None:
        self.software: str = context.ENVIRONMENT["SOFTWARE"]
        self.basename: str = context.ENVIRONMENT["BASENAME"]
        self.extension: str = self._file_extention()
        self.new_topology_file: Path = context.environment_menager.data_dir / (
            self.basename + self.extension
        )
        if self.new_topology_file.exists():
            context.environment_menager.make_duplicate(self.new_topology_file)

        self.logger.info("Writing paramters.")

    @override
    def __call__(self, context: MDContext, next_step: NextStep) -> None:
        self.__precall__(context)

    def _file_extention(self) -> str:
        if self.software == "amber":
            return ".parm7"

        if self.software == "gromacs":
            return ".top"
        raise ValueError(f"Unsupported simulation software: {self.software}")


class WriteCoordinates(TopologyReadInterface):
    def __init__(self, index: int = 0) -> None:
        self.logger: logging.Logger = logging.getLogger(self.__class__.__name__)
        self.index: int = index

    def __precall__(self, context: MDContext) -> None:
        self.software: str = context.ENVIRONMENT["SOFTWARE"]
        self.basename: str = context.ENVIRONMENT["BASENAME"]
        self.extension: str = self._file_extention()
        self.new_coordinates_file: Path = context.environment_menager.data_dir / (
            self.basename + f"-{self.index}" + self.extension
        )
        if self.new_coordinates_file.exists():
            context.environment_menager.make_duplicate(self.new_coordinates_file)

        self.logger.info("Writing coordinates.")

    @override
    def __call__(self, context: MDContext, next_step: NextStep) -> None:
        self.__precall__(context)
        try:
            context.STRUCTURE.save(str(self.new_coordinates_file), overwrite=True)
            context.CURRENT_RUN["START_COORDINATES_FILE"] = self.new_coordinates_file

            self.logger.debug(
                f"Writing coordinates to file {self.new_coordinates_file}"
            )
            next_step(context)
        except Exception as e:
            raise IOError(
                f"Error writing coordinates to file {self.new_coordinates_file}: {e}"
            )

    def _file_extention(self) -> str:
        """Determine the file extension based on the simulation software."""
        if self.software == "amber":
            return ".rst7"
        if self.software == "gromacs":
            return ".gro"
        raise ValueError(f"Unsupported simulation software: {self.software}")


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
