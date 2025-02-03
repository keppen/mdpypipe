import logging
from pathlib import Path
from typing import Any, override

from parmed import Structure

from src.context.context import MDContext
from src.interfaces.datatypes import (
    RunConfig,
    TopolConfig,
    TypedDictError,
    castTopolConfig,
    issectiondict,
)
from src.interfaces.pipeline import NextStep, PipeStep, Pipeline
from src.logger import log_json


class MergeTopologies(PipeStep[MDContext]):
    def __init__(self) -> None:
        self.logger: logging.Logger = logging.getLogger(self.__class__.__name__)

    def __precall__(self, context: MDContext) -> None:
        self.software: str = context.ENVIRONMENT["SOFTWARE"]
        self.topol_configs: list[TopolConfig] = context.simulation_menager.topol_configs
        self.structure: Structure = Structure()  # Explicitly annotate the type here
        self.index: int = 1
        self.name: str = ""
        self.ff: str = ""
        self.number: int = 0
        self.file: Path = context.environment_menager.data_dir / (
            context.ENVIRONMENT["BASENAME"] + self._file_extention()
        )

        self.logger.info(f"Creating new topology file {str(self.file)}.")

    @override
    def __call__(self, context: MDContext, next_step: NextStep) -> None:
        self.__precall__(context)
        new_topolgy = self._merge_topologies()
        context.simulation_menager.topol_configs.append(new_topolgy)
        log_json(
            self.logger,
            "Topol configs",
            {k: str(v) for k, v in new_topolgy.items()},
        )

        self.logger.info("Topology created and added: " + str(self.structure))
        next_step(context)

    def _merge_topologies(
        self,
    ) -> TopolConfig:
        for topology in self.topol_configs:
            if topology["TOPOLOGY"] is not None:
                self.structure += topology["TOPOLOGY"]
                self.index += 1
                self.name += f"{topology['NAME']} | "
                self.ff += f"{topology['FF']} | "
                self.number += topology["NUMBER"]

        topology_config: dict[str, Any] = {
            "INDEX": self.index,
            "NAME": self.name,
            "FF": self.ff,
            "NUMBER": self.number,
            "FILE": self.file,
            "TOPOLOGY": self.structure,
        }

        if not issectiondict(TopolConfig, topology_config):
            raise TypedDictError(
                "New topology configuration is not of TopolConfig type"
            )
        return castTopolConfig(topology_config)

    def _file_extention(self) -> str:
        if self.software == "amber":
            return ".parm7"

        if self.software == "gromacs":
            return ".top"
        raise ValueError(f"Unsupported simulation software: {self.software}")


class SetCurrentTopology(PipeStep[MDContext]):
    def __init__(self, topol_config: TopolConfig) -> None:
        self.logger: logging.Logger = logging.getLogger(self.__class__.__name__)
        self.topol_config: TopolConfig = topol_config

    @override
    def __call__(self, context: MDContext, next_step: NextStep) -> None:
        context.CURRENT_TOPOL = self.topol_config
        log_json(
            self.logger,
            "Topol configs",
            {k: str(v) for k, v in self.topol_config.items()},
        )
        self.logger.info(f"Current topology set - index {self.topol_config['INDEX']}")
        return next_step(context)


class SetCurrentRun(PipeStep[MDContext]):
    def __init__(self, run_config: RunConfig) -> None:
        self.logger: logging.Logger = logging.getLogger(self.__class__.__name__)
        self.run_config: RunConfig = run_config

    @override
    def __call__(self, context: MDContext, next_step: NextStep) -> None:
        context.CURRENT_RUN = self.run_config
        log_json(
            self.logger,
            "Topol configs",
            {k: str(v) for k, v in self.run_config.items()},
        )
        self.logger.info(f"Current run config set - index {self.run_config['INDEX']}")
        return next_step(context)


class FindFiles(PipeStep[MDContext]):
    def __init__(self) -> None:
        self.logger: logging.Logger = logging.getLogger(self.__class__.__name__)

    @override
    def __call__(self, context: MDContext, next_step: NextStep) -> None:
        return next_step(context)


class FindRunConfig(PipeStep[MDContext]):
    def __init__(self, coordinates_file: str, config_file: str) -> None:
        self.logger: logging.Logger = logging.getLogger(self.__class__.__name__)

        self.coordindates_file: Path = Path(coordinates_file)
        self.config_file: Path = Path(config_file)

    @override
    def __call__(self, context: MDContext, next_step: NextStep) -> None:
        for run_config in context.simulation_menager.run_configs:
            config_file_i = run_config["CONFIG_FILE"]
            coordinates_file_i = run_config["START_COORDINATES_FILE"]
            if (
                self.coordindates_file.name == coordinates_file_i.name
                and self.config_file.name == config_file_i.name
            ):
                pipe: Pipeline[MDContext] = Pipeline(SetCurrentRun(run_config))
                pipe(context)
        return next_step(context)


class FindTopolConfig(PipeStep[MDContext]):
    def __init__(self, topol_file: str) -> None:
        self.logger: logging.Logger = logging.getLogger(self.__class__.__name__)

        self.topol_file: Path = Path(topol_file)

    @override
    def __call__(self, context: MDContext, next_step: NextStep) -> None:
        for topol_config in context.simulation_menager.topol_configs:
            topol_file_i = topol_config["FILE"]
            if self.topol_file.name == topol_file_i.name:
                pipe: Pipeline[MDContext] = Pipeline(SetCurrentTopology(topol_config))
                pipe(context)
        return next_step(context)
