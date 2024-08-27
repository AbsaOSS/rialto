from typing import Dict

from loguru import logger


def _override(config, path, value) -> Dict:
    key = path[0]
    if "[" in key:
        name = key.split("[")[0]
        index = key.split("[")[1].replace("]", "")
        if "=" in index:
            index_key, index_value = index.split("=")
            position = next(i for i, x in enumerate(config[name]) if x.get(index_key) == index_value)
            if len(path) == 1:
                config[name][position] = value
            else:
                config[name][position] = _override(config[name][position], path[1:], value)
        else:
            index = int(index)
            if index >= 0:
                if len(path) == 1:
                    config[name][index] = value
                else:
                    config[name][index] = _override(config[name][index], path[1:], value)
            else:
                if len(path) == 1:
                    config[name].append(value)
                else:
                    raise ValueError(f"Invalid index {index} for key {name} in path {path}")
    else:
        if len(path) == 1:
            config[key] = value
        else:
            config[key] = _override(config[key], path[1:], value)
    return config


def override_config(config: Dict, overrides: Dict) -> Dict:
    """Override config with user input

    :param config: Config dictionary
    :param overrides: Dictionary of overrides
    :return: Overridden config
    """
    for path, value in overrides.items():
        logger.info("Applying override: ", path, value)
        config = _override(config, path.split("."), value)

    return config
