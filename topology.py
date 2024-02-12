import parmed as pmd
from parmed import unit as u
from pipeline import NextStep
from interfaces import PipeStepInterface
from context import ContextMD
import os


class ReadTopology(PipeStepInterface):
    def __init__(self, molecule_name: str) -> None:
        self.molecule_name = molecule_name
        self.step_name = ["LOAD_TOPOLOGY", self.molecule_name]

    def __call__(self, context: ContextMD, next_step: NextStep) -> None:
        msg = ":".join(self.step_name)
        context.do_step(msg)

        context.STRUCTURE = context.STRUCTURE + self.structure

        print(f"STEPS: {context.STEPS_HISTORY}")
        next_step(context)

    def read_topology(self, file: str, ff: str) -> None:
        software = self.check_extention(file)
        self.step_name.extend([file, ff])
        if software == "amber":
            self.structure = pmd.amber.AmberFormat(file)
        if software == "gmx":
            self.structure = pmd.gromacs.GromacsTopologyFile(file)
            if len(self.structure.residues) > 1:
                print("There are more than one residue. Extra steps are nessesary.")
        print(self.structure)


class ReadCoordinates(PipeStepInterface):
    def __init__(self, system_name: str) -> None:
        self.system_name = system_name
        self.step_name = ["LOAD_COORDFILE", self.system_name]

    def __call__(self, context: ContextMD, next_step: NextStep):
        msg = ":".join(self.step_name)
        context.do_step(msg)

        if context.STRUCTURE.box is None:
            context.STRUCTURE.box = self.coord_data.box
        context.STRUCTURE.positions = self.coord_data.positions

        print(f"STEPS: {context.STEPS_HISTORY}")
        next_step(context)

    def read_coordinates(self, file: str):
        software = self.check_extention(file)
        self.step_name.append(file)
        if software == "amber":
            self.coord_data = pmd.amber.Rst7(file)
        if software == "gmx":
            self.coord_data = pmd.gromacs.GromacsGroFile.parse(file)


class WriteParameters(PipeStepInterface):
    def __init__(self, basename: str, software: str) -> None:
        self.software = software
        self.basename = basename
        self.step_name = ["WRITTING_PARAMS", self.basename]
        self._init_extentions()

    def __call__(self, context: ContextMD, next_step: NextStep) -> None:
        msg = ":".join(self.step_name)
        context.do_step(msg)

        context.STRUCTURE.save(
            os.path.join(context.DATA_DIR, self.basename) +
            self.params_file_ext,
            overwrite=True,
        )

        print(f"STEPS: {context.STEPS_HISTORY}")
        next_step(context)

    def _init_extentions(self) -> None:
        if self.software == "amber":
            self.params_file_ext = ".parm7"

        if self.software == "gromacs":
            self.params_file_ext = ".top"


class WriteCoordinates(PipeStepInterface):
    def __init__(self, basename: str, software: str) -> None:
        self.software = software
        self.basename = basename
        self.step_name = ["WRITTING_COORDS", self.basename]
        self._init_extentions()

    def __call__(self, context: ContextMD, next_step: NextStep) -> None:
        msg = ":".join(self.step_name)
        context.do_step(msg)

        context.STRUCTURE.save(
            os.path.join(context.DATA_DIR, self.basename) +
            self.coord_file_ext,
            overwrite=True,
        )

        print(f"STEPS: {context.STEPS_HISTORY}")
        next_step(context)

    def _init_extentions(self) -> None:
        if self.software == "amber":
            self.coord_file_ext = ".rst7"

        if self.software == "gromacs":
            self.coord_file_ext = ".gro"


if __name__ == "__main__":
    import pipeline as pip
    import interfaces as intf
    import context as cnx

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

    context = cnx.ContextMD("A1")
    job1 = ReadTopology("Mol1")
    job1.read_topology("test/a1_test.top", "Amber14SB")
    print(job1.step_name)
    job2 = ReadTopology("Mol2")
    job2.read_topology("test/chcl3.top", "GAFF")
    job3 = ReadCoordinates("a1_chcl3")
    job3.read_coordinates("test/10ns_a1-1.gro")
    basename = "a1_chcl3"
    job4 = WriteParameters(basename, "gromacs")
    job5 = WriteCoordinates(basename, "gromacs")
    pipe: pip.Pipeline = pip.Pipeline(job1, job2, job3, job4, job5)
    pipe(context)
