import logging
import re
from pathlib import Path
from typing import (
    Any,
    TypeVar,
    get_type_hints,
    get_args,
)
from src.interfaces.datatypes import (
    SectionDict,
    SlurmConfig,
    EnvironmentConfig,
    RunConfig,
    TopolConfig,
    DatabaseConfig,
    issectiondict,
    isoptional,
    TypedDictError,
    castRunConfig,
    castTopolConfig,
    castEnvironment,
    castSlurmConfig,
    castDatabaseConfig,
)


T = TypeVar("T")
ConfigType = dict[str, dict[str, str]]


class Parser:
    def __init__(self, file: Path):
        self.logger: logging.Logger = logging.getLogger(self.__class__.__name__)
        self.logger.debug(f"Initializing {self.__class__.__name__}.")

        self.file: Path = file
        self.sections_typed: list[SectionDict] = []
        self.config_data: ConfigType = self._parse_config(self.file)

    def from_config(
        self,
    ) -> tuple[
        EnvironmentConfig,
        DatabaseConfig,
        list[TopolConfig],
        list[RunConfig],
        SlurmConfig | None,
    ]:
        run_configs: list[RunConfig] = []
        topol_configs: list[TopolConfig] = []
        slurm_config: SlurmConfig | None = None
        environment_config: EnvironmentConfig | None = None
        database_config: DatabaseConfig | None = None
        for key, config_dict in self.config_data.items():
            section_dict = self.to_section_dict(
                self._find_section(key), key, config_dict
            )
            if issectiondict(SlurmConfig, section_dict):
                slurm_config = castSlurmConfig(section_dict)
            elif issectiondict(EnvironmentConfig, section_dict):
                environment_config = castEnvironment(section_dict)
            elif issectiondict(DatabaseConfig, section_dict):
                database_config = castDatabaseConfig(section_dict)
            elif issectiondict(RunConfig, section_dict):
                run_configs.append(castRunConfig(section_dict))
            elif issectiondict(TopolConfig, section_dict):
                topol_configs.append(castTopolConfig(section_dict))
            else:
                raise TypedDictError(
                    f"Section key {key} has not been recognized as one of sections."
                )

        if not environment_config:
            raise TypedDictError("Environment config section has not been set.")
        if not database_config:
            raise TypedDictError("Database config section has not been set.")
        if not topol_configs:
            raise TypedDictError("Topol config sections have not been set.")
        if not run_configs:
            raise TypedDictError("Run config sections have not been set.")

        return (
            environment_config,
            database_config,
            topol_configs,
            run_configs,
            slurm_config,
        )

    def to_section_dict(
        self,
        typed_section: type[SectionDict],
        section_name: str,
        section_dict: dict[str, str],
    ) -> dict[str, Any]:
        for option, _ in section_dict.items():
            if option not in get_type_hints(typed_section).keys():
                raise KeyError(f"Option {option} was not found in {section_name}.")

        section_typeddict: dict[str, Any] = {}
        field_type: type[Any]
        field_type_list: tuple[Any, ...]
        for field_name, field_type in get_type_hints(typed_section).items():
            value = self._get_field_value(
                section_dict, section_name, field_name, field_type
            )

            if isoptional(field_type):
                field_type_list = get_args(field_type)
            else:
                field_type_list = (field_type,)

            if value is not None:
                if int in field_type_list and self._isint(value):
                    section_typeddict[field_name] = int(value)
                elif Path in field_type_list and self._ispathlike(value):
                    section_typeddict[field_name] = Path(value)
                elif str in field_type_list:
                    section_typeddict[field_name] = str(value)
                else:
                    continue
            else:
                section_typeddict[field_name] = value

        if not issectiondict(typed_section, section_typeddict):
            raise TypedDictError(
                f"Dictionary do not match the structure of {typed_section}"
            )

        return section_typeddict

    def _get_field_value(
        self,
        section_dict: dict[str, str],
        section_name: str,
        field_name: str,
        field_type: type[Any],
    ) -> str | None:
        """Handle fetching and validating the field value."""
        if field_name in section_dict:
            return section_dict[field_name]

        if isoptional(field_type):
            return None

        match_title = re.match(r"[A-Z]+", section_name)
        if match_title is not None:
            if match_title.group(0) in ["RUNMD", "TOPOL"]:
                match_index = re.search(r"\d+", section_name)
                if match_index:
                    return str(match_index.group())
                else:
                    raise ValueError(
                        f"Unable to find an index in section name '{section_name}'."
                    )

        raise ValueError(
            f"Field '{field_name}' in section '{section_name}' is not optional and is missing."
        )

    @staticmethod
    def _ispathlike(string: str) -> bool:
        regex = r"^\w+\.\w+|^(\/\w+)+\.\w+|^(\w+\/)+\w+.\w+|^(\/\w+)+"
        if re.search(regex, string):
            return True
        return False

    @staticmethod
    def _isint(string: str) -> bool:
        return string.isdigit()

    @staticmethod
    def _parse_config(file: Path) -> ConfigType:
        with open(file, "r") as config_file:
            lines = config_file.readlines()
        config_data: ConfigType = {}

        section: str = ""  # Initialize as None
        section_dict: dict[str, str] = {}  # Initialize as None

        in_bracket = False
        for line in lines:
            line = line.strip()
            if not line:
                continue
            if line.startswith("{"):
                in_bracket = True
                section = line.split()[1]
                section_dict = {}
                continue
            if line.startswith("}"):
                config_data[section.upper()] = section_dict
                in_bracket = False

            if in_bracket:
                key: str
                value: str
                if line.startswith(";"):
                    continue
                key, _, value, *_ = list(map(str.strip, line.split()))
                section_dict[key.upper()] = value

        return config_data

    def _find_section(self, section_name: str) -> type[SectionDict]:
        section_map: dict[str, type[SectionDict]] = {
            "ENVIRONMENT": EnvironmentConfig,
            "SLURM": SlurmConfig,
            "RUNMD": RunConfig,
            "TOPOL": TopolConfig,
            "DATABASE": DatabaseConfig,
        }

        for key in section_map.keys():
            if section_name == key or section_name.startswith(key):
                return section_map[key]

        raise KeyError(f"Section '{section_name}' not found.")


if __name__ == "__main__":
    context = Parser(Path("/home/keppen/MD/parameters/amber-test.config"))

    # context.from_config(Path("/home/keppen/MD/parameters/amber-test.config"))

    # try:
    #     value: Any = section_dict[field_name]
    # except KeyError:
    #     if not hasattr(field_type, "__origin__"):
    #         if section_name in ["RUNMD", "TOPOL"]:
    #             value = re.match("$d+", section_name)
    #         else:
    #             raise ValueError(f"No index found in {section_name}.")
    #     else:
    #         raise ValueError(f"{field_name} is not optional.")
    #
    #     if (
    #         get_origin(field_type) is Union
    #         and type(None) in field_type.__args__
    #     ):
    #         value = None
    #     else:
    #         raise ValueError(f"{field_name} cannot be None.")
