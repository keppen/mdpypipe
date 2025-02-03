import os
import subprocess
import logging
from pathlib import Path
from typing import Any, TypedDict


class SubprocessKwargs(TypedDict, total=False):
    text: bool
    env: dict[str, str]
    stdout: int | None
    stderr: int | None
    capture_output: bool


class SSHConnection:
    cmd: list[str] = []

    def __init__(self, ssh_adress: str, ssh_dir: Path):
        self.logger: logging.Logger = logging.getLogger(self.__class__.__name__)
        self.logger.info("Initializing SSH connection")

        self.ssh_adress: str = ssh_adress
        self.ssh_dir: Path = ssh_dir

        self.error: bool = False
        self.subprocess_kwargs: SubprocessKwargs = {
            "text": True,
            "env": {**os.environ, "SSH_AUTH_SOCK": os.environ.get("SSH_AUTH_SOCK", "")},
            "stdout": subprocess.PIPE,
            "stderr": subprocess.PIPE,
        }

        force: bool = False

        self.connection: bool = self._check_connection()
        if self.connection:
            self.logger.info("Connection set.")
        else:
            self.logger.error("Connecrtion has not been set.")
            exit(1)
        if force:
            self.connection = True
            self.logger.warning("Connection is foreced!!!")

    def _check_connection(self) -> bool:
        ssh_sock = self.subprocess_kwargs.get("env", {}).get("SSH_AUTH_SOCK")
        if not ssh_sock or not Path(ssh_sock).exists():
            self.logger.error("ssh-agent is not set or the socket does not exist!")
            self.logger.error('Run "eval `ssh-agent` && ssh-add"')
            return False

        self.cmd = ["ssh-add", "-L"]
        process: subprocess.CompletedProcess[str] = self._run_command()

        if process.returncode != 0:
            self.logger.error("ssh-agent has been unset!")
            self.logger.error('Run "eval `ssh-agent` && ssh-add"')
            return False
        else:
            self.logger.info("Connection is OK")
            self.cmd = []
            return True

    def _run_command(self) -> subprocess.CompletedProcess[str]:
        try:
            process: subprocess.CompletedProcess[str] = subprocess.run(
                self.cmd,
                shell=False,  # Prefer False for security unless you need shell features
                **self.subprocess_kwargs,
            )
            self.logger.debug(f"COMMAND\t{' '.join(self.cmd)}")
            self.logger.debug(f"STDOUT\t{process.stdout.strip()}")
            self.logger.debug(f"STDERR\t{process.stderr.strip()}")

            if self._error_check(process):
                self.error = True
            return process
        except subprocess.SubprocessError as e:
            self.logger.error(f"Error while running command: {e}")
            self.error = True
            raise subprocess.SubprocessError

    def _error_check(self, process: subprocess.CompletedProcess[str]) -> bool:
        if process.returncode != 0:
            # self.logger.debug(f"STDERR\t{process.stderr.strip()}")
            return True
        return False

    def send_files(self, src: str, dest: str) -> None:
        self.cmd = ["scp", "-r", str(src), str(dest)]
        self.logger.info(f"Files has been sent via SSH: {' '.join(self.cmd)}")
        _ = self._run_command()

    def run_remotely(self, command: str) -> subprocess.CompletedProcess[Any]:
        self.cmd = ["ssh", self.ssh_adress, command]
        self.logger.info(f"Commnad via SSH has been evoked: {' '.join(self.cmd)}")
        process: subprocess.CompletedProcess[str] = self._run_command()
        return process

    def run_locally(self, cmd: list[str]) -> subprocess.CompletedProcess[Any]:
        self.cmd = cmd
        self.logger.info(f"Commnad has been evoked locally: {' '.join(self.cmd)}")
        process: subprocess.CompletedProcess[str] = self._run_command()
        return process


# if __name__ == "__main__":
# ssh = SSHConnection()._check_connection()
