import logging
import os
from pathlib import Path
from typing import Any

import pandas as pd
import parmed as pmd

from src.context.database import Database
from src.context.ssh_connection import SSHConnection
from src.context.config_parser import Parser
from src.interfaces.datatypes import (
    SlurmConfig,
    EnvironmentConfig,
    RunConfig,
    TopolConfig,
    DatabaseConfig,
    issectiondict,
    TypedDictError,
    castTopolConfig,
)
from src.logger import log_json


class DatabaseMenager:
    database_config: DatabaseConfig

    def __init__(self, database_config: DatabaseConfig) -> None:
        self.logger: logging.Logger = logging.getLogger(self.__class__.__name__)
        self.logger.info(f"Initializing {self.__class__.__name__}.")
        log_json(
            self.logger,
            "Database config",
            {k: str(v) for k, v in database_config.items()},
        )

        self.database_config = database_config
        self.database_config["DATABASE"] = self._init_database()

    def _init_database(self) -> Database:
        self.logger.info("Initializing database object.")
        database_path = self.database_config["DATABASE_PATH"]
        if database_path.exists():
            self.logger.info(f"The database exists at {database_path}")

            return Database(database_path)
        else:
            self.logger.warning(
                f"The database path has not been set. Creating new one at {database_path}."
            )

            columns = [
                "ROOT DIR",
                "PROJECT NAME",
                "SIMULATION NAME",
                "TOPOLOGY FILE",
                "COORDINATE FILE",
                "CONFIG FILE",
                "STAGE",
                "REMOTE ADRESS",
                "REMOTE DIR",
                "LUSTRE DIR",
                "PID",
            ]
            return Database.from_scratch(database_path, columns)

    @property
    def database(self) -> Database:
        db = self.database_config.get("DATABASE")
        if not db:
            raise ValueError("Database object has not been instantiated.")
        return db

    def add_entry(
        self,
        index: int,
        simulation_name: str,
        root: str,
        project_name: str,
        remote_address: str,
        remote_dir: str,
        lustre_dir: str,
        current_topol_file: str,
        current_coordinates_file: str,
        current_config_file: str,
        pid: str,
    ):
        new_entry: dict[str, str] = {
            "ROOT DIR": root,
            "PROJECT NAME": project_name,
            "SIMULATION NAME": simulation_name,
            "TOPOLOGY FILE": current_topol_file,
            "COORDINATE FILE": current_coordinates_file,
            "CONFIG FILE": current_config_file,
            "STAGE": "Unfinished",
            "REMOTE ADRESS": remote_address,
            "REMOTE DIR": remote_dir,
            "LUSTRE DIR": lustre_dir,
            "PID": pid,
        }
        self.database.add_entry(index, new_entry)

    def find_entries(self, query: dict[str, str]) -> pd.DataFrame:
        return self.database.find_entries(query)

    def get_last_index(self) -> int:
        if self.database.database.empty:
            return 1
        return self.database.database.index[-1]

    def modify_entry(self, to_modify: tuple[str, str], query: dict[str, str]) -> None:
        self.database.modify_entry(to_modify, query)


class SlurmMenager:
    slurm_config: SlurmConfig

    def __init__(self, slurm_config: SlurmConfig) -> None:
        self.logger: logging.Logger = logging.getLogger(self.__class__.__name__)
        self.logger.debug(f"Initializing {self.__class__.__name__}.")
        log_json(
            self.logger,
            "Slurm config",
            {k: str(v) for k, v in slurm_config.items()},
        )

        self.slurm_config = slurm_config
        self.slurm_config["SSH_CONNECTION"] = SSHConnection(
            self.slurm_config["REMOTE_ADRESS"], self.slurm_config["REMOTE_DIR"]
        )

        self.init_download_dir()

    @property
    def ssh_connection(self) -> SSHConnection:
        if not self.slurm_config["SSH_CONNECTION"]:
            raise ValueError("SSH connection has not been configured.")
        if not self.slurm_config["SSH_CONNECTION"].connection:
            raise ValueError("SSH contection has not been established.")
        return self.slurm_config["SSH_CONNECTION"]

    @property
    def connection(self) -> bool:
        if not self.slurm_config["SSH_CONNECTION"]:
            raise ValueError("SSH configuration was not performed.")
        return self.slurm_config["SSH_CONNECTION"].connection

    def init_download_dir(self) -> None:
        self.logger.info("Validating the existence of download directory.")

        download_dir = self.slurm_config["DOWNLOAD_DIR"]

        if download_dir.exists():
            self.logger.warning(f"Directory '{str(download_dir)}' already exists!")
        else:
            download_dir.mkdir(parents=True)
            self.logger.info(f"Directory '{str(download_dir)}' has been created.")
        self.logger.info("OK")


class EnvironmentMenager:
    environment: EnvironmentConfig

    def __init__(self, environment: EnvironmentConfig):
        self.logger: logging.Logger = logging.getLogger(self.__class__.__name__)
        self.logger.info(f"Initializing {self.__class__.__name__}.")
        log_json(
            self.logger,
            "Environment config",
            {k: str(v) for k, v in environment.items()},
        )

        self.environment = environment

        if self.environment["ROOT"] is None:
            self.environment["ROOT"] = Path(os.getcwd())

        if self.environment["DATA_DIR"] is None:
            assert self.environment["ROOT"] is not None
            self.environment["DATA_DIR"] = (
                self.environment["ROOT"] / self.environment["PROJECT_NAME"]
            )
        if self.environment["PARAM_DIR"] is None:
            self.environment["PARAM_DIR"] = self.environment["ROOT"]

        self._init_dirs()

    def _init_dirs(self):
        self.logger.info("Validating the existence of essential directories.")

        for directory in [self.root_dir, self.param_dir]:
            if not directory.exists():
                raise FileNotFoundError(
                    f"Required directory '{str(directory)}' does not exist."
                )

        if self.data_dir.exists():
            self.logger.warning(f"Directory '{str(self.data_dir)}' already exists!")
        else:
            self.data_dir.mkdir(parents=True)
            self.logger.info(f"Directory '{str(self.data_dir)}' has been created.")
        self.logger.info("OK")

    @property
    def root_dir(self) -> Path:
        root = self.environment["ROOT"]
        if not root:
            raise ValueError("Root path has not been instantiated.")
        return root

    @property
    def param_dir(self) -> Path:
        param = self.environment["PARAM_DIR"]
        if not param:
            raise ValueError("Param path has not been instantiated.")
        return param

    @property
    def data_dir(self) -> Path:
        data = self.environment["DATA_DIR"]
        if not data:
            raise ValueError("Data path has not been instantiated.")
        return data

    def make_absolute(self, file: Path) -> Path:
        if file.is_absolute():
            self.logger.debug(f"{file} found.")
            return file

        directories_to_check = [self.root_dir, self.param_dir, self.data_dir]
        for directory in directories_to_check:
            if (directory / file).exists():
                self.logger.debug(
                    f"{file} found at {directory}. Changing path appropriately."
                )
                return directory / file
        raise FileNotFoundError(f"{file} has not been found.")

    def make_duplicate(self, file: Path) -> None:
        ndup = 0
        for duplicate in os.listdir(self.data_dir):
            if duplicate.startswith(file.name):
                ndup += 1

        if ndup == 0:
            self.logger.debug("No duplicate was found.")
            return None

        duplicate_path: Path = self.data_dir / f"{str(file)}.bck{ndup}"
        self.logger.debug(
            f"Making duplicate of {str(file.name)}. Duplicate number {ndup}."
        )
        os.rename(file, duplicate_path)


class SimulationMenager:
    topol_configs: list[TopolConfig]
    run_configs: list[RunConfig]
    current_topol_config: TopolConfig | None
    current_run_config: RunConfig | None

    TOPOLOGY_EXT: dict[str, str] = {"gromas": ".top", "amber": ".parm7"}
    POSITION_EXT: dict[str, str] = {"gromas": ".gro", "amber": ".rst7"}

    def __init__(
        self,
        run_configs: list[RunConfig],
        topol_configs: list[TopolConfig],
    ) -> None:
        self.logger: logging.Logger = logging.getLogger(self.__class__.__name__)
        self.logger.info(f"Initializing {self.__class__.__name__}.")
        for run_config in run_configs:
            log_json(
                self.logger, "Run configs", {k: str(v) for k, v in run_config.items()}
            )

        for topol_config in topol_configs:
            log_json(
                self.logger,
                "Topol configs",
                {k: str(v) for k, v in topol_config.items()},
            )

        self.topol_configs = self._sortTopolConfigs(topol_configs)
        self.run_configs = self._sortRunConfigs(run_configs)

        self.current_topol_config = None
        self.current_run_config = None

    @property
    def ENRG_GROUPS(self) -> list[str]:
        if not self.current_topol_config:
            raise ValueError("Currnet topolconfig is not set.")
        if not self.current_topol_config["TOPOLOGY"]:
            raise ValueError("Structure object of currnet topology has not been set.")
        enrg_groups: list[str] = []
        resnames: list[str] = [
            residue.name for residue in self.current_topol_config["TOPOLOGY"].residues
        ]
        enrg_groups.extend(set(resnames))
        return enrg_groups

    @property
    def STRUCTURE(self) -> pmd.Structure:
        if not self.current_run_config:
            raise ValueError("Current runconfig is not set.")
        if not self.current_topol_config:
            raise ValueError("Currnet topolconfig is not set.")

        structure = self.current_topol_config["TOPOLOGY"]
        if structure is not None:
            structure.positions = self.current_run_config["START_COORDINATES"]
            structure.box = self.current_run_config["START_BOX"]
            return structure
        else:
            raise ValueError("Current topology has no Strucutre object set.")

    @staticmethod
    def _sortTopolConfigs(config: list[TopolConfig]) -> list[TopolConfig]:
        return sorted(config, key=lambda x: x["INDEX"])

    @staticmethod
    def _sortRunConfigs(config: list[RunConfig]) -> list[RunConfig]:
        return sorted(config, key=lambda x: x["INDEX"])


class MDContext:
    slurm_menager: SlurmMenager | None
    database_menager: DatabaseMenager
    environment_menager: EnvironmentMenager
    simulation_menager: SimulationMenager

    def __init__(
        self,
        environment_config: EnvironmentConfig,
        database_config: DatabaseConfig,
        topol_configs: list[TopolConfig],
        run_configs: list[RunConfig],
        slurm_config: SlurmConfig | None = None,
    ) -> None:
        self.logger: logging.Logger = logging.getLogger(self.__class__.__name__)
        self.logger.debug(f"Initializing {self.__class__.__name__}.")

        self.environment_menager = EnvironmentMenager(environment_config)

        if slurm_config:
            self.slurm_menager = SlurmMenager(slurm_config)
        else:
            self.slurm_menager = None

        # database_config = self._check_files_database_config(database_config)
        topol_configs = self._check_files_topol_configs(topol_configs)
        run_configs = self._check_files_run_configs(run_configs)

        self.database_menager = DatabaseMenager(database_config)
        self.simulation_menager = SimulationMenager(run_configs, topol_configs)

    @classmethod
    def from_config(cls, file: Path) -> "MDContext":
        parser = Parser(file)
        return cls(*parser.from_config())

    @property
    def ENVIRONMENT(self) -> EnvironmentConfig:
        return self.environment_menager.environment

    @property
    def SLURM(self) -> SlurmConfig:
        if not self.slurm_menager:
            raise ValueError("Slurm section has not been set.")
        return self.slurm_menager.slurm_config

    @property
    def DATABASE(self) -> DatabaseConfig:
        return self.database_menager.database_config

    @property
    def CURRENT_TOPOL(self) -> TopolConfig:
        if not self.simulation_menager.current_topol_config:
            raise ValueError("Current topology config is not set.")
        return self.simulation_menager.current_topol_config

    @CURRENT_TOPOL.setter
    def CURRENT_TOPOL(self, topol_config: TopolConfig) -> None:
        self.simulation_menager.current_topol_config = topol_config

    @property
    def CURRENT_RUN(self) -> RunConfig:
        if not self.simulation_menager.current_run_config:
            raise ValueError("Current run config is not set.")
        return self.simulation_menager.current_run_config

    @CURRENT_RUN.setter
    def CURRENT_RUN(self, run_config: RunConfig) -> None:
        self.simulation_menager.current_run_config = run_config

    @property
    def ENRG_GROUPS(self) -> list[str]:
        return self.simulation_menager.ENRG_GROUPS

    @property
    def STRUCTURE(self) -> pmd.Structure:
        return self.simulation_menager.STRUCTURE

    @property
    def CONNECTION(self) -> bool:
        if not self.slurm_menager:
            return False
        return self.slurm_menager.connection

    def add_entry(self, index: int, simulation_name: str) -> None:
        paths_root: str = str(self.environment_menager.root_dir)
        project_name: str = self.environment_menager.environment["PROJECT_NAME"]

        if self.slurm_menager:
            remote_address: str = self.SLURM["REMOTE_ADRESS"]
            remote_dir: str = str(self.SLURM["REMOTE_DIR"])
            lustre_dir: str = str(self.SLURM["LUSTRE_DIR"])
            pid: str = str(self.SLURM["PID"])
        else:
            remote_dir = "None"
            remote_address = "None"
            lustre_dir = "None"
            pid = "-1"

        current_topol_file: str = str(self.CURRENT_TOPOL["FILE"].name)
        current_coordinates_file: str = str(
            self.CURRENT_RUN["START_COORDINATES_FILE"].name
        )
        current_config_file: str = str(self.CURRENT_RUN["CONFIG_FILE"].name)

        self.database_menager.add_entry(
            index=index,
            simulation_name=simulation_name,
            root=paths_root,
            project_name=project_name,
            remote_address=remote_address,
            remote_dir=remote_dir,
            lustre_dir=lustre_dir,
            current_topol_file=current_topol_file,
            current_coordinates_file=current_coordinates_file,
            current_config_file=current_config_file,
            pid=pid,
        )

    def find_unfinished(self) -> pd.DataFrame:
        query: dict[str, Any] = {
            "PROJECT NAME": self.ENVIRONMENT["PROJECT_NAME"],
            "STAGE": "Unfinished",
        }
        return self.database_menager.find_entries(query)

    def find_downloaded(self) -> pd.DataFrame:
        query: dict[str, Any] = {
            "PROJECT NAME": self.ENVIRONMENT["PROJECT_NAME"],
            "STAGE": "DOWNLOADED",
        }
        return self.database_menager.find_entries(query)

    # def change_pid(self, pid: int) -> None:
    #     self.database_menager.change_pid(pid)

    def modify_entry(self, to_modify: tuple[str, str], query: dict[str, str]) -> None:
        self.database_menager.modify_entry(to_modify, query)

    def find_index(self, simulation_name: str) -> int:
        query: dict[str, Any] = {
            "PROJECT NAME": self.ENVIRONMENT["PROJECT_NAME"],
            "SIMULATION NAME": simulation_name,
        }
        found_run: pd.DataFrame = self.database_menager.find_entries(query)

        if found_run.empty:
            return self.database_menager.get_last_index() + 1
        elif len(found_run) > 1:
            raise ValueError(f"More than one index entries.\n {found_run}")
        else:
            return found_run.index[0]

    def _check_files_database_config(
        self, database_config: DatabaseConfig
    ) -> DatabaseConfig:
        database_config["DATABASE_PATH"] = self.environment_menager.make_absolute(
            database_config["DATABASE_PATH"]
        )
        return database_config

    def _check_files_topol_configs(
        self, topol_configs: list[TopolConfig]
    ) -> list[TopolConfig]:
        for index, _ in enumerate(topol_configs):
            topol_configs[index]["FILE"] = self.environment_menager.make_absolute(
                topol_configs[index]["FILE"]
            )
        return topol_configs

    def _check_files_run_configs(self, run_configs: list[RunConfig]) -> list[RunConfig]:
        for index, _ in enumerate(run_configs):
            if index == 0:
                run_configs[index]["START_COORDINATES_FILE"] = (
                    self.environment_menager.make_absolute(
                        run_configs[index]["START_COORDINATES_FILE"]
                    )
                )
            run_configs[index]["CONFIG_FILE"] = self.environment_menager.make_absolute(
                run_configs[index]["CONFIG_FILE"]
            )
        return run_configs


if __name__ == "__main__":
    # topology_gro = pmd.gromacs.GromacsTopologyFile("test/a1.top")
    # print(topology_gro)
    # topology_gro.write("test.top")
    #
    # gmx_a1 = pmd.gromacs.GromacsTopologyFile("test/a1_alone.top")
    # print(gmx_a1)
    # gmx_chcl3 = pmd.gromacs.GromacsTopologyFile("test/chcl3.top")
    # print(gmx_chcl3)
    # gmx_system: pmd.Structure = gmx_a1 + gmx_chcl3
    # print(gmx_system)
    # gro_system = pmd.gromacs.GromacsGroFile.parse("test/10ns_a1-1.gro")
    # print(gro_system)
    # print(gro_system.residues)

    # gmx_a1.write("test/a1_test.top")

    # list_methods = [m for m in dir(gmx_a1)]
    # for method in list_methods:
    #     print(method)

    context = MDContext.from_config(
        Path("/home/keppen/MD/parameters/amber-test.config")
    )
