from typing import List
import os
from abc import ABC, abstractmethod
from pipeline import NextStep
import parmed as pmd
from context import ContextMD


# class ContextInterface(ABC):
#     STEPS_HISTORY: List[str] = []
#     STRUCTURE: pmd.Structure = pmd.Structure()
#
#     def do_step(self, step_name: str) -> None:
#         self.STEPS_HISTORY.append(step_name)


class PipeStepInterface(ABC):
    gmx_ext: List[str] = ["gro", "top", "itp"]
    amber_ext: List[str] = ["parm7", "prmtop", "inpcrd", "rst7", "restrt"]
    step_name: List[str]

    @abstractmethod
    def __call__(self, context: ContextMD, next_step: NextStep) -> None:
        ...

    def check_extention(self, file: str) -> str:
        filename, extention = os.path.basename(file).split(".")
        print(filename, extention)
        if extention in self.amber_ext:
            return "amber"
        if extention in self.gmx_ext:
            return "gmx"
        raise Exception(f"Wrong {extention} extention")
