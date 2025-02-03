import logging
import json
from logging import Logger


def log_json(logger: Logger, message: str, kwargs: dict[str, str]) -> None:
    def type_string(x: str) -> str:
        return str(type(x)).split("'")[1]

    logger.debug(
        message + ": %s",
        json.dumps(
            {k: f"{str(v)} : {type_string(v)}".format() for k, v in kwargs.items()},
            indent=4,
        ),
    )


# set up logging to file - see previous section for more details
logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s %(name)-24s %(levelname)-8s %(message)s",
    datefmt="%m-%d %H:%M",
    filename="myapp.log",
    filemode="w",
)
# define a Handler which writes INFO messages or higher to the sys.stderr
console = logging.StreamHandler()
console.setLevel(logging.INFO)
# set a format which is simpler for console use
formatter = logging.Formatter("%(name)-24s: %(levelname)-8s %(message)s")
# tell the handler to use this format
console.setFormatter(formatter)
# add the handler to the root logger
logging.getLogger("").addHandler(console)
