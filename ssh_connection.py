import os
import subprocess
import logging
from pathlib import Path
from typing import Any


class SSHConnection:
    def __init__(self, ssh_adress: str, ssh_dir: Path):
        self.logger = logging.getLogger(__name__)
        self.logger.info("Initializing SSH connection")
        self.ssh_adress = ssh_adress
        self.ssh_dir = ssh_dir

        self.error = False
        self.subprocess_kargs: dict[str, Any] = {
            "text": True,
            "env": {"SSH_AUTH_SOCK": os.environ.get("SSH_AUTH_SOCK")},
        }

        self._check_connection()

    def _check_connection(self):
        if self.subprocess_kargs["env"]["SSH_AUTH_SOCK"] is None:
            self.logger.error("ssh-agent is not set!")
            self.logger.error('run "eval `ssh-agent` && ssh-add"')

        self.cmd = ["ssh-add", "-L"]
        process = self._run_command(**self.subprocess_kargs)

        # self.cmd = ["ssh", self.ssh_adress, "-o", "BatchMode=yes", "exit"]
        # self._run_command(**self.subprocess_kargs)

        if process.returncode != 0:
            self.logger.error("ssh-agent has been unset!")
            self.logger.error('Run "eval `ssh-agent` && ssh-add"')
        else:
            self.logger.info("Connection is OK")
            self.cmd = []

    def _run_command(self, **kargs) -> subprocess.CompletedProcess:
        process = subprocess.run(
            " ".join(self.cmd),
            shell=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            **kargs,
        )
        self.logger.debug(f"COMMAND\t{process.args.strip()}")
        self.logger.debug(f"STDOUT\t{process.stdout.strip()}")

        if self._error_check(process):
            self.error = True
        return process

    def _error_check(self, process: subprocess.CompletedProcess) -> bool:
        if process.returncode != 0:
            self.logger.debug(f"STDERR\t{process.stderr.strip()}")
            return True
        return False

    def send_files(self, src: str, dest: str) -> None:
        self.cmd = ["scp", src, dest]
        self._run_command(**self.subprocess_kargs)
        # if self.error:
        #     print("There was an error.")

    def run_remotely(self, command: str) -> subprocess.CompletedProcess:
        self.cmd = ["ssh", self.ssh_adress, command]
        process = self._run_command(**self.subprocess_kargs)
        # if self.error:
        #     print("There was an error.")
        return process


# if __name__ == "__main__":
# ssh = SSHConnection()._check_connection()
