import os
import subprocess
from pathlib import Path


class SSHConnection:
    def __init__(self, ssh_adress: str, ssh_dir: Path):
        self.ssh_adress = ssh_adress
        self.ssh_dir = ssh_dir

        self.error = False
        self.subprocess_kargs = {
            "text": True,
            "env": {"SSH_AUTH_SOCK": os.environ.get("SSH_AUTH_SOCK")},
        }

        self._check_connection()

    def _check_connection(self):
        self.cmd = ["ssh-add", "-L"]
        self._run_command(**self.subprocess_kargs)

        # self.cmd = ["ssh", self.ssh_adress, "-o", "BatchMode=yes", "exit"]
        # self._run_command(**self.subprocess_kargs)

        if self.error:
            print("CONNECTIONS WAS NOT SECURED!")
        else:
            self.cmd = []
            print("Connection is OK")

    def _run_command(self, **kargs) -> subprocess.CompletedProcess:
        process = subprocess.run(
            " ".join(self.cmd),
            shell=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            **kargs,
        )
        print(process.args)
        print(process.stdout)
        if self._error_check(process):
            self.error = True
        return process

    def _error_check(self, process: subprocess.CompletedProcess) -> bool:
        if process.returncode != 0:
            print("COMMAND:\t", " ".join(self.cmd))
            print(process.stderr)
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
