import os

import pytest
import yaml

from rialto.common.env_yaml import EnvLoader


def test_plain():
    data = {"a": "string_value", "b": 2}
    cfg = """
    a: string_value
    b: 2
    """
    assert yaml.load(cfg, EnvLoader) == data


def test_full_sub_default():
    data = {"a": "default_value", "b": 2}
    cfg = """
    a: ${EMPTY_VAR:default_value}
    b: 2
    """
    assert yaml.load(cfg, EnvLoader) == data


def test_full_sub_env():
    os.environ["FILLED_VAR"] = "env_value"
    data = {"a": "env_value", "b": 2}
    cfg = """
    a: ${FILLED_VAR:default_value}
    b: 2
    """
    assert yaml.load(cfg, EnvLoader) == data


def test_partial_sub_start():
    data = {"a": "start_string", "b": 2}
    cfg = """
    a: ${START_VAR:start}_string
    b: 2
    """
    assert yaml.load(cfg, EnvLoader) == data


def test_partial_sub_end():
    data = {"a": "string_end", "b": 2}
    cfg = """
    a: string_${END_VAR:end}
    b: 2
    """
    assert yaml.load(cfg, EnvLoader) == data


def test_partial_sub_mid():
    data = {"a": "string_mid_sub", "b": 2}
    cfg = """
    a: string_${MID_VAR:mid}_sub
    b: 2
    """
    assert yaml.load(cfg, EnvLoader) == data


def test_partial_sub_no_default_no_value():
    with pytest.raises(Exception) as e:
        cfg = """
        a: string_${MANDATORY_VAL_MISSING}_sub
        b: 2
        """
        assert yaml.load(cfg, EnvLoader)
    assert str(e.value) == "Environment variable MANDATORY_VAL_MISSING has no assigned value"


def test_partial_sub_no_default():
    os.environ["MANDATORY_VAL"] = "mandatory_value"
    data = {"a": "string_mandatory_value_sub", "b": 2}
    cfg = """
    a: string_${MANDATORY_VAL}_sub
    b: 2
    """
    assert yaml.load(cfg, EnvLoader) == data
