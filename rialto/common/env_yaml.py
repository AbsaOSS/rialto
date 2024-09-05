import os
import re

import yaml
from loguru import logger

__all__ = ["EnvLoader"]

# Regex pattern to capture variable and the rest of the string
_path_matcher = re.compile(r"(?P<before>.*)\$\{(?P<env_name>[^}^{:]+)(?::(?P<default_value>[^}^{]*))?\}(?P<after>.*)")


def _path_constructor(loader, node):
    value = node.value
    match = _path_matcher.search(value)
    if match:
        before = match.group("before")
        after = match.group("after")
        sub = os.getenv(match.group("env_name"), match.group("default_value"))
        if sub is None:
            raise ValueError(f"Environment variable {match.group('env_name')} has no assigned value")
        new_value = before + sub + after
        logger.info(f"Config: Replacing {value}, with {new_value}")
        return new_value
    return value


class EnvLoader(yaml.SafeLoader):
    """Custom loader that replaces values with environment variables"""

    pass


EnvLoader.add_implicit_resolver("!env_substitute", _path_matcher, None)
EnvLoader.add_constructor("!env_substitute", _path_constructor)
