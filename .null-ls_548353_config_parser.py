import logging
import re
from pathlib import Path
from typing import (
    Any,
    Dict,
    List,
    Type,
    TypeVar,
    Union,
    get_type_hints,
    get_origin,
    cast,
)
from datatypes import (
    SectionDict,
    SlurmConfig,
    Environment,
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

from logger import log_json


T = TypeVar("T")


class Parser:
    def __init__(self, file: Path):
        logger = logging.getLogger(__name__)
        logger.info(f"Setting up context from file {str(file)}")

        self.file = file
        self.sections_typed: List[SectionDict] = []
        self.config_data = self._parse_config(self.file)
        self.from_config()

    def from_config(self) -> None:
        for key, config_dict in self.config_data.items():
            section_dict = self.to_section_dict(
                self._find_section(key), key, config_dict
            )
            print("Section dictionary", section_dict, "\n\n")
        slurm_section: SlurmConfig
        # self.sections_typed.append(section_dict)
        # for section in self.sections_typed:
        #     print(type(section))
        #     print()

    def to_section_dict(
        self,
        typed_section: Type[SectionDict],
        section_name: str,
        section_dict: Dict[str, str],
    ) -> Dict[str, Any]:
        for option, _ in section_dict.items():
            if option not in get_type_hints(typed_section).keys():
                raise KeyError(f"Option {option} was not found in {section_name}.")

        converted_options: Dict[str, Any] = {}
        for field_name, field_type in get_type_hints(typed_section).items():
            value = self._get_field_value(
                section_dict, section_name, field_name, field_type
            )
            if isoptional(field_type):
                field_type_list = field_type.__args__
            else:
                field_type_list = [field_type]

            if value is not None:
                if int in field_type_list and self._isint(value):
                    converted_options[field_name] = int(value)
                elif Path in field_type_list and self._ispathlike(value):
                    converted_options[field_name] = Path(value)
                elif str in field_type_list:
                    converted_options[field_name] = str(value)
                else:
                    continue
            else:
                continue

        if not issectiondict(typed_section, converted_options):
            raise TypedDictError(
                f"Dictionary do not match the structure of {typed_section}"
            )
        # try:
        #     slurm_dict = casttypedict(converted_options)
        #     slurm_dict["x"] = 1
        #     reveal_type(slurm_dict)
        # except TypedDictError:
        #     pass

        return converted_options

    def _get_field_value(
        self,
        section_dict: Dict[str, str],
        section_name: str,
        field_name: str,
        field_type: Any,
    ) -> Any:
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
        regex = r"^(\/\w+\/)|(\.{1,2}\/)+|\w+\/"
        if re.search(regex, string):
            return True
        return False

    @staticmethod
    def _isint(string: str) -> bool:
        return string.isdigit()

    @staticmethod
    def _parse_config(file: Path) -> Dict[str, Any]:
        with open(file, "r") as config_file:
            lines = config_file.readlines()
        config_data: Dict[str, Dict[str, str]] = {}

        in_bracket = False
        for line in lines:
            line = line.strip()
            if not line:
                continue
            if line.startswith("{"):
                in_bracket = True
                section = line.split()[1]
                section_dict: Dict[str, str] = {}
                continue
            if line.startswith("}"):
                config_data[section.upper()] = section_dict
                in_bracket = False

            if in_bracket:
                key: str
                value: str
                if line.startswith(";"):
                    continue
                key, _, value, *comments = list(map(str.strip, line.split()))
                section_dict[key.upper()] = value

        return config_data

    def _find_section(self, section_name: str) -> Type[SectionDict]:
        section_map: Dict[str, Type[SectionDict]] = {
            "ENVIRONMENT": Environment,
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
