import os
import subprocess
import stat
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Protocol

from src.context.context import MDContext
from src.interfaces.pipeline import NextStep, PipeStep


# class PipeStepInterface(ABC):
#     @abstractmethod
#     def __call__(self, context: MDContext, next_step: NextStep) -> None: ...


class ShellInterface(PipeStep[MDContext], ABC):
    error: bool = False
    cmd: list[str]

    @abstractmethod
    def gen_command(self, *args: str, **kwargs: str) -> None: ...

    def _run_command(self, cmd: list[str]) -> subprocess.CompletedProcess[str]:
        """Run a shell command and handle errors."""
        try:
            process = subprocess.run(
                cmd,
                shell=False,
                text=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )
            if self._is_error(process):
                self.error = True
            return process
        except subprocess.SubprocessError as e:
            raise RuntimeError(f"Failed to execute command: {' '.join(cmd)}\n{e}")

    def _is_error(self, process: subprocess.CompletedProcess[str]) -> bool:
        """Check if the command execution resulted in an error."""
        if process.returncode != 0:
            return True
        return False

    @staticmethod
    def make_executable(file_path: Path) -> None:
        """Make a file executable."""
        try:
            st = os.stat(file_path)
            os.chmod(file_path, st.st_mode | stat.S_IEXEC)
        except OSError as e:
            raise PermissionError(f"Failed to make {file_path} executable: {e}")


class TopologyReadInterface(PipeStep[MDContext], ABC):
    """Interface for steps that read topology files."""

    GMX_EXTENSIONS: list[str] = [".gro", ".top", ".itp"]
    AMBER_EXTENSIONS: list[str] = [".parm7", ".prmtop", ".inpcrd", ".rst7", ".restrt"]

    def _check_extension(self, file: Path) -> str:
        """Determine the software type based on file extension."""
        _, extension = os.path.splitext(file)
        if extension in self.AMBER_EXTENSIONS:
            return "amber"
        if extension in self.GMX_EXTENSIONS:
            return "gromacs"
        raise ValueError(f"Unsupported file extension: {extension}")
