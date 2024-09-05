import os
import re

import yaml
from loguru import logger

__all__ = ["EnvLoader"]

_path_matcher = re.compile(r"\$\{(?P<env_name>[^}^{:]+)(?::(?P<default_value>[^}^{]*))?\}")


def _path_constructor(loader, node):
    value = node.value
    match = _path_matcher.match(value)
    sub = os.getenv(match.group("env_name"), match.group("default_value"))
    new_value = value[0 : match.start()] + sub + value[match.end() :]
    logger.info(f"Config: Replacing {value}, with {new_value}")
    return new_value


class EnvLoader(yaml.SafeLoader):
    """Custom loader that replaces values with environment variables"""

    pass


EnvLoader.add_implicit_resolver("!env_substitute", _path_matcher, None)
EnvLoader.add_constructor("!env_substitute", _path_constructor)
