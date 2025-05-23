#  Copyright 2022 ABSA Group Limited
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

__all__ = ["override_config"]

from typing import Dict, List, Tuple

from loguru import logger


def _split_index_key(key: str) -> Tuple[str, str]:
    name = key.split("[")[0]
    index = key.split("[")[1].replace("]", "")
    return name, index


def _find_first_match(config: List, index: str) -> int:
    index_key, index_value = index.split("=")
    return next(i for i, x in enumerate(config) if x.get(index_key) == index_value)


def _override(config, path, value) -> Dict:
    key = path[0]
    if "[" in key:
        name, index = _split_index_key(key)
        if name not in config:
            raise ValueError(f"Invalid key: {name}")
        if "=" in index:
            index = _find_first_match(config[name], index)
        else:
            index = int(index)
        if index >= 0 and index < len(config[name]):
            if len(path) == 1:
                config[name][index] = value
            else:
                config[name][index] = _override(config[name][index], path[1:], value)
        elif index == -1:
            if len(path) == 1:
                config[name].append(value)
            else:
                raise ValueError(f"Invalid index {index} for key {name} in path {path}")
        else:
            raise IndexError(f"Index {index} out of bounds for key {key}")
    else:
        if len(path) == 1:
            if key not in config:
                logger.warning(f"Adding new key: {key} with value {value}")
            config[key] = value
        else:
            if key not in config:
                raise ValueError(f"Invalid key: {key}")
            config[key] = _override(config[key], path[1:], value)
    return config


def override_config(config: Dict, overrides: Dict) -> Dict:
    """Override config with user input

    :param config: config dictionary
    :param overrides: dictionary of overrides
    :return: Overridden config
    """
    for path, value in overrides.items():
        logger.info(f"Applying override:\npath: {path}\nvalue: {value}")
        config = _override(config, path.split("."), value)

    return config
