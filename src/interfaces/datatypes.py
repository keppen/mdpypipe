from pathlib import Path
from traceback import print_tb
from types import UnionType
from typing import (
    Any,
    Type,
    TypedDict,
    Union,
    cast,
    get_args,
    get_origin,
    get_type_hints,
)

from numpy.typing import ArrayLike
import parmed as pmd

from src.context.database import Database
from src.context.ssh_connection import SSHConnection


class TypedDictError(Exception):
    """Custom exception for TypedDict validation errors."""

    def __init__(self, message: str):
        super().__init__(message)


class TypedConfig(TypedDict):
    pass


class SlurmConfig(TypedConfig):
    REMOTE_DIR: Path
    LUSTRE_DIR: Path
    DOWNLOAD_DIR: Path
    REMOTE_ADRESS: str
    NODES: int
    NTASKS: int
    MEMORY: str
    TIME: str
    ACCOUNT: str
    PARTITION: str
    QOS: str | None
    GPU_RESOURCES: str | None
    NGPU: int | None
    SSH_CONNECTION: "SSHConnection | None"
    PID: int | None


class DatabaseConfig(TypedConfig):
    DATABASE_PATH: Path
    DATABASE: "Database | None"


class TopolConfig(TypedConfig):
    INDEX: int
    NAME: str
    FF: str
    NUMBER: int
    FILE: Path
    TOPOLOGY: pmd.Structure | None


class RunConfig(TypedConfig):
    INDEX: int
    SIM_TYPE: str
    NRUNS: int
    CONFIG_FILE: Path
    START_COORDINATES_FILE: Path
    START_COORDINATES: pmd.unit.Quantity | None
    START_BOX: ArrayLike | None


class EnvironmentConfig(TypedConfig):
    SOFTWARE: str
    BASENAME: str
    PROJECT_NAME: str
    RESOURCE: str
    CPUS_PER_TASK: int
    ROOT: Path | None
    PARAM_DIR: Path | None
    DATA_DIR: Path | None


SectionDict = SlurmConfig | DatabaseConfig | TopolConfig | RunConfig | EnvironmentConfig


def isoptional(field_type: type[Any]) -> bool:
    origin = get_origin(field_type)
    if origin in [Union, UnionType]:
        if type(None) in get_args(field_type):
            return True
    return False


def issectiondict(obj: type, instance: dict[str, Any]) -> bool:
    fields: dict[str, type[Any]] = get_type_hints(obj)

    for field_name, field_type in fields.items():
        if field_name not in instance:
            return False

        value: str = instance[field_name]

        origin = get_origin(field_type)
        args: tuple[type[Any], ...] = get_args(field_type)
        if origin in [Union, UnionType]:  # Handle `Union` types
            if not any(_is_instance(value, arg) for arg in args):
                return False
        elif origin:
            if not isinstance(value, origin):
                return False
        else:
            if not isinstance(value, field_type):
                return False

    return True


def _is_instance(value: str, expected_type: type[Any]) -> bool:
    """
    Helper function to handle isinstance checks for both simple and generic types.
    """
    origin = get_origin(expected_type)
    if origin:
        return isinstance(value, origin)
    return isinstance(value, expected_type)


def castSlurmConfig(instance: dict[str, Any]) -> SlurmConfig:
    if not issectiondict(SlurmConfig, instance):
        raise TypedDictError("Not a SlurmConfigDict")

    return cast(SlurmConfig, instance)  # pyright: ignore[reportInvalidCast]


def castDatabaseConfig(instance: dict[str, Any]) -> DatabaseConfig:
    if not issectiondict(DatabaseConfig, instance):
        raise TypedDictError("Not a DatabaseConfigDict")

    return cast(DatabaseConfig, instance)  # pyright: ignore[reportInvalidCast]

    # return DatabaseConfig(
    #     DATABASE_PATH=Path(instance["DATABASE_PATH"]),
    #     DATABASE=instance.get("DATABASE"),
    # )


def castTopolConfig(instance: dict[str, Any]) -> TopolConfig:
    if not issectiondict(TopolConfig, instance):
        raise TypedDictError("Not a TopolConfigDict")

    return cast(TopolConfig, instance)  # pyright: ignore[reportInvalidCast]
    # return TopolConfig(
    #     INDEX=int(instance["INDEX"]),
    #     NAME=str(instance["NAME"]),
    #     FF=str(instance["FF"]),
    #     NUMBER=int(instance["NUMBER"]),
    #     FILE=Path(instance["FILE"]),
    #     TOPOLOGY=instance.get("TOPOLOGY"),
    # )


def castRunConfig(instance: dict[str, Any]) -> RunConfig:
    if not issectiondict(RunConfig, instance):
        raise TypedDictError("Not a RunConfigDict")

    return cast(RunConfig, instance)  # pyright: ignore[reportInvalidCast]
    # return RunConfig(
    #     INDEX=int(instance["INDEX"]),
    #     SIM_TYPE=str(instance["SIM_TYPE"]),
    #     NRUNS=int(instance["NRUNS"]),
    #     CONFIG_FILE=Path(instance["CONFIG_FILE"]),
    #     START_POSITIONS_FILE=Path(instance["START_POSITIONS_FILE"]),
    #     START_BOX_FILE=Path(instance["START_BOX_FILE"]),
    #     START_POSITIONS=instance.get("START_POSITIONS"),
    #     START_BOX=instance.get("START_BOX"),
    # )


def castEnvironment(instance: dict[str, Any]) -> EnvironmentConfig:
    if not issectiondict(EnvironmentConfig, instance):
        raise TypedDictError("Not an EnvironmentDict")

    return cast(EnvironmentConfig, instance)  # pyright: ignore[reportInvalidCast]
    # return EnvironmentConfig(
    #     SOFTWARE=str(instance["SOFTWARE"]),
    #     BASENAME=str(instance["BASENAME"]),
    #     PROJECT_NAME=str(instance["PROJECT_NAME"]),
    #     ROOT=instance.get("ROOT"),
    #     PARAM_DIR=instance.get("PARAM_DIR"),
    #     DATA_DIR=instance.get("DATA_DIR"),
    # )
