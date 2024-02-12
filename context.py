import os
import parmed as pmd
from typing import List
# from interfaces import ContextInterface


class ContextMD:
    __ROOT_DIR: str | None
    __DATA_DIR: str
    __PARAM_DIR: str
    __BASENAME: str

    STEPS_HISTORY: List[str] = []
    STRUCTURE: pmd.Structure = pmd.Structure()

    def __init__(self, basename: str, root: str | None = None) -> None:
        self.__BASENAME = basename
        self.__ROOT_DIR = root if root else os.getcwd()
        self.DATA_DIR = os.path.join(self.__ROOT_DIR, self.__BASENAME)
        self.__PARAM_DIR = os.path.abspath("/home/keppen/MD/parameters/")
        self._init_dir()

    def _init_dir(self):
        if not os.path.exists(self.__PARAM_DIR):
            print(
                f"Directory {self.__PARAM_DIR} does not extist. Change parameter directory"
            )
            exit(1)
        if not os.path.exists(self.__ROOT_DIR):
            print(
                f"Root directory {self.__ROOT_DIR} does not exists. Change root directory."
            )
            exit(1)
        if not os.path.exists(self.DATA_DIR):
            print(f"Directory {self.DATA_DIR} does not extist. Creating one.")
            os.mkdir(self.DATA_DIR)

    def do_step(self, step_name: str) -> None:
        self.STEPS_HISTORY.append(step_name)


if __name__ == "__main__":
    topology_gro = pmd.gromacs.GromacsTopologyFile("test/a1.top")
    print(topology_gro)
    topology_gro.write("test.top")

    gmx_a1 = pmd.gromacs.GromacsTopologyFile("test/a1_alone.top")
    print(gmx_a1)
    gmx_chcl3 = pmd.gromacs.GromacsTopologyFile("test/chcl3.top")
    print(gmx_chcl3)
    gmx_system = gmx_a1 + gmx_chcl3
    print(gmx_system)
    gro_system = pmd.gromacs.GromacsGroFile.parse("test/10ns_a1-1.gro")
    print(gro_system)
    print(gro_system.residues)

    # gmx_a1.write("test/a1_test.top")

    # list_methods = [m for m in dir(gmx_a1)]
    # for method in list_methods:
    #     print(method)
