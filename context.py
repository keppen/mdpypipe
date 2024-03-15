import os
import re
import pandas as pd
from pathlib import Path
import shutil
import parmed as pmd
from typing import List, Dict, Any, Iterable, Mapping
from numpy.typing import ArrayLike
from database import Database
from ssh_connection import SSHConnection
from dataclasses import dataclass, field

# DatabaseType = Type[Database]
# StructureType = Type[pmd.Structure]


@dataclass
class ContextMD:
    TITLE_SOFTWARE: str
    TITLE_BASENAME: str
    TITLE_PROJECT_NAME: str
    SLURM_NODES: int
    SLURM_CORES: int
    SLURM_MEMORY: str
    SLURM_TIME: str
    SLURM_ACCOUNT: str
    SLURM_PARTITION: str
    SLURM_QOS: str
    SLURM_GPU_RESOURCES: str
    SLURM_RESOURCE: str
    GEOMETRY_POSITIONS_FILE: Path
    GEOMETRY_BOX_FILE: Path

    PATHS_ROOT: Path = Path(os.getcwd())
    PATHS_REMOTE_ADRESS: str = "mszatko@ui.wcss.pl"
    PATHS_REMOTE_DIR: Path = Path("/home/mszatko/MD/test")
    PATHS_DATABASE_PATH: Path = Path("/home/keppen/MD/parameters")
    PATHS_PARAM_DIR: Path = Path("/home/keppen/MD/parameters")

    CURRENT_TOPFILE: Path = field(init=False)
    CURRENT_POSFILE: Path = field(init=False)
    CURRENT_CONFIGFILE: Path = field(init=False)
    DATABASE: "Database" = field(init=False)
    SSH_CONNECTION: "SSHConnection" = field(init=False)
    PID: int = field(init=False)

    STEPS_HISTORY: List[str] = field(init=False, default_factory=list)
    TOPOLOGIES: Dict[str, pmd.Structure] = field(init=False, default_factory=dict)
    POSITIONS: pmd.unit.Quantity = field(init=False, default_factory=pmd.unit.Quantity)
    BOX: ArrayLike = field(init=False)

    TOP_EXT = {"gromas": ".top", "amber": ".parm7"}
    POS_EXT = {"gromas": ".gro", "amber": ".rst7"}

    def __post_init__(self) -> None:
        self.PATHS_DATA_DIR = self.PATHS_ROOT / self.TITLE_PROJECT_NAME
        self.PATHS_REMOTE_DIR = self.PATHS_REMOTE_DIR / self.TITLE_PROJECT_NAME
        self.DATABASE = self._init_database()
        self.SSH_CONNECTION = SSHConnection(
            self.PATHS_REMOTE_ADRESS, self.PATHS_REMOTE_DIR
        )
        self._init_dirs()

    def _init_dirs(self):
        for dir_attr in ["ROOT", "DATA_DIR", "PARAM_DIR"]:
            dir_path = getattr(self, f"PATHS_{dir_attr}")
            if not dir_path.exists():
                os.mkdir(dir_path)

    def _init_database(self) -> Database:
        if self.PATHS_DATABASE_PATH.exists():
            print("The database exists")
            return Database(self.PATHS_DATABASE_PATH)
        else:
            print("The database does not exist")
        columns = [
            "ROOT DIR",
            "PROJECT NAME",
            "REMOTE ADRESS",
            "REMOTE DIR",
            "SIMULATION NAME",
            "TOPOLOGY FILE",
            "POSITIONS FILE",
            "CONFIG FILE",
            "STAGE",
            "HISTORY",
            "PID",
        ]
        return Database.from_scratch(self.PATHS_DATABASE_PATH, columns)

    @classmethod
    def from_config(cls, file: Path) -> "ContextMD":
        config_data = cls._parse_config(file)
        native_args, unexpected_args = {}, {}

        for key, value in config_data.items():
            if cls._is_int(value):
                value = int(value)
            elif cls._is_pathlike(value):
                value = Path(value)

            if key in cls.__annotations__:
                native_args[key] = value
            else:
                unexpected_args[key] = value

        data_cls = cls(**native_args)

        for key, value in unexpected_args.items():
            data_cls.__dict__[key] = value

        return data_cls

    @staticmethod
    def _parse_config(file: Path) -> Dict[str, Any]:
        with open(file, "r") as config_file:
            lines = config_file.readlines()
        config_data: Dict[str, Any] = {}

        in_bracket = False
        for line in lines:
            line = line.strip()
            if line.startswith("{"):
                in_bracket = True
                section = line.split()[1]
                continue
            if line.startswith("}"):
                in_bracket = False

            if in_bracket:
                key: str
                value: str
                if line.startswith(";"):
                    continue
                key, _, value = list(map(str.strip, line.split()))
                config_data[f"{section.upper()}_{key.upper()}"] = value

        return config_data

    @property
    def ENRG_GROUPS(self) -> List[str]:
        enrg_groups: List[str] = []
        for _, structure in self.TOPOLOGIES.items():
            resnames = [residue.name for residue in structure.residues]
            enrg_groups.extend(set(resnames))
        return enrg_groups

    @property
    def STRUCTURE(self) -> pmd.Structure:
        if not self.POSITIONS or self.BOX is None:
            return None

        structure = pmd.Structure()
        for topology in self.TOPOLOGIES.values():
            structure += topology
        structure.positions = self.POSITIONS
        structure.box = self.BOX
        return structure

    @property
    def SLURM_CONFIG(self) -> Dict[str, Any]:
        slurm_config: Dict[str, Any] = {}
        for key, value in self.__dict__.items():
            if "SLURM" in key:
                new_key = key.replace("SLURM_", "").lower()
                slurm_config[new_key] = value
        slurm_config["software"] = self.TITLE_SOFTWARE
        return slurm_config

    @property
    def RUNMD_CONFIG(self) -> List[Dict[str, Any]]:
        config_list: List[Dict[str, Any]] = []

        prefixes: List[str] = []
        for attribute in list(self.__dict__.keys()):
            match = re.search(r"^RUNMD\d+", attribute)
            if match:
                prefixes.append(match.group())

        for prefix in self._sort_strings(set(prefixes)):
            single_runmd = self._set_single_config(prefix)

            single_runmd["software"] = self.TITLE_SOFTWARE
            single_runmd["number"] = 0
            single_runmd["topology_file"] = self.CURRENT_TOPFILE

            single_runmd["positions_file"] = self.PATHS_ROOT / self._set_pos_file(
                prefix, config_list
            )

            config_list.append(single_runmd)

        return config_list

    def _set_pos_file(self, prefix: str, config_list: List[Dict[str, Any]]) -> str:
        if prefix == "RUNMD1":
            pos_filename = self.CURRENT_POSFILE.name
        else:
            nruns = len(config_list)
            prev_sim_type = config_list[nruns - 1]["sim_type"]
            prev_number = config_list[nruns - 1]["number"]
            ext = self.POS_EXT[self.TITLE_SOFTWARE]

            pos_filename = f"{prev_number}-{prev_sim_type}.{ext}"
        return pos_filename

    def _set_single_config(self, prefix: str) -> Dict[str, Any]:
        single_config: Dict[str, Any] = {}
        kword_match = f"{prefix}_"
        for key, value in list(self.__dict__.items()):
            if kword_match in key:
                new_key = key.replace(kword_match, "").lower()
                single_config[new_key] = value

        return single_config

    @property
    def TOP_CONFIG(self) -> List[Dict[str, Any]]:
        config_list: List[Dict[str, Any]] = []

        prefixes: List[str] = []
        for attribute in list(self.__dict__.keys()):
            if re.search(r"^TOPOL\d+_", attribute):
                prefixes.append(attribute.split("_")[0])

        for prefix in self._sort_strings(set(prefixes)):
            single_top = self._set_single_config(prefix)

            config_list.append(single_top)

        return config_list

    @staticmethod
    def _sort_strings(str_list: Iterable[str]) -> Iterable[str]:
        def match_digits(string: str) -> int:
            match = re.search(r"\d+$", string)
            if match:
                return int(match.group())
            else:
                return -1

        return sorted(str_list, key=match_digits)

    @staticmethod
    def _is_pathlike(string: str) -> bool:
        regex = r"^((\/\w+\/)|(\.{1,2}\/)+|\w+\/)"
        if re.search(regex, string):
            return True
        return False

    @staticmethod
    def _is_int(string: str) -> bool:
        return string.isdigit()

    def add_entry(self, index: int, simulation_name: str) -> None:
        new_line_dict: Dict[str, Any] = {
            "ROOT DIR": self.PATHS_ROOT,
            "PROJECT NAME": self.TITLE_PROJECT_NAME,
            "REMOTE ADRESS": self.PATHS_REMOTE_ADRESS,
            "REMOTE DIR": self.PATHS_REMOTE_DIR,
            "SIMULATION NAME": simulation_name,
            "TOPOLOGY FILE": self.CURRENT_TOPFILE.name,
            "POSITIONS FILE": self.CURRENT_POSFILE.name,
            "CONFIG FILE": self.CURRENT_CONFIGFILE.name,
            "STAGE": "Unfinished",
            "HISTORY": ":".join(self.STEPS_HISTORY),
            "PID": -1,
        }

        self.DATABASE.add_entry(index, new_line_dict)

    def find_unfinished(self) -> pd.DataFrame:
        sim_kwargs = {
            "PROJECT NAME": self.TITLE_PROJECT_NAME,
            "STAGE": "Unfinished",
        }
        return self.DATABASE.find_entries(**sim_kwargs)

    def change_pid(self, pid: int):
        self.PID = pid
        self.DATABASE.tmp_database["PID"] = pid

    def find_index(self, simulation_name) -> int:
        sim_kwargs = {
            "PROJECT NAME": self.TITLE_PROJECT_NAME,
            "SIMULATION NAME": simulation_name,
        }
        found_run = self.DATABASE.find_entries(**sim_kwargs)

        if found_run.empty:
            return len(self.DATABASE.database) + len(self.DATABASE.tmp_database)
        elif len(found_run) > 1:
            raise Exception
        else:
            return found_run.index[0]

    def move_files(self) -> None:
        for key, src_path in self.__dict__.items():
            if not isinstance(src_path, Path):
                continue
            if not src_path.exists() or src_path.is_dir():
                continue
            dest_path = self.PATHS_DATA_DIR / os.path.basename(src_path)
            if dest_path.exists():
                os.remove(dest_path)
            shutil.copy(src_path, dest_path)
            self.__dict__[key] = dest_path

    def remove_file(self, file_name: str):
        file_path = self.PATHS_DATA_DIR / file_name
        if file_path.exists():
            os.remove(file_path)

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
