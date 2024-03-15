import os
from abc import ABC, abstractmethod
from typing import Callable, List, Mapping, Any, AnyStr, Iterable

from pathlib import Path
from context import ContextMD
from pipeline import NextStep
import subprocess

# class ContextInterface(ABC):
#     STEPS_HISTORY: List[str] = []
#     STRUCTURE: pmd.Structure = pmd.Structure()
#
#     def do_step(self, step_name: str) -> None:
#         self.STEPS_HISTORY.append(step_name)


def verbose_call(call_function: Callable) -> Callable:
    def wrapper(self, context: ContextMD, next_step: NextStep) -> None:
        msg = ":".join(self.step_name)
        context.do_step(msg)
        print(f"STEPS DONE: {context.STEPS_HISTORY}")

        call_function(self, context, next_step)

    return wrapper


class PipeStepInterface(ABC):
    error: bool = False
    step_name: List[str]

    @abstractmethod
    def __call__(self, context: ContextMD, next_step: NextStep) -> None: ...


class ShellInterface(PipeStepInterface):
    cmd: List[str]

    @abstractmethod
    def gen_command(self, *agrs, **kwargs): ...

    def _run_command(self, cmd: List[str], **kwargs) -> subprocess.CompletedProcess:
        process = subprocess.run(
            " ".join(cmd),
            shell=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            **kwargs,
        )
        print(process.stdout)
        if self._error_code(process):
            self.error = True
        return process

    def _error_code(self, process: subprocess.CompletedProcess) -> bool:
        if process.returncode != 0:
            print("COMMAND:\t", " ".join(process.args))
            print(process.stderr)
            return True
        return False

    def _make_executable(self, file: Path) -> None:
        os.chmod(file, 0o777)


class TopologyReadInterface(PipeStepInterface):
    gmx_ext: List[str] = [".gro", ".top", ".itp"]
    amber_ext: List[str] = [".parm7", ".prmtop", ".inpcrd", ".rst7", ".restrt"]

    def _check_extention(self, file: os.PathLike) -> str:
        filename, extention = os.path.splitext(file)
        if extention in self.amber_ext:
            return "amber"
        if extention in self.gmx_ext:
            return "gromacs"
        raise Exception(f"Wrong {extention} extention")
