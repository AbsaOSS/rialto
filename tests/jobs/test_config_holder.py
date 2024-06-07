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


from datetime import date

import pytest

from rialto.jobs.configuration.config_holder import (
    ConfigException,
    ConfigHolder,
    FeatureStoreConfig,
)


def test_run_date_unset():
    with pytest.raises(ConfigException):
        ConfigHolder.get_run_date()


def test_run_date():
    dt = date(2023, 1, 1)

    ConfigHolder.set_run_date(dt)

    assert ConfigHolder.get_run_date() == dt


def test_feature_store_config_unset():
    with pytest.raises(ConfigException):
        ConfigHolder.get_feature_store_config()


def test_feature_store_config():
    ConfigHolder.set_feature_store_config("store_schema", "metadata_schema")

    fsc = ConfigHolder.get_feature_store_config()

    assert type(fsc) is FeatureStoreConfig
    assert fsc.feature_store_schema == "store_schema"
    assert fsc.feature_metadata_schema == "metadata_schema"


def test_config_unset():
    config = ConfigHolder.get_config()

    assert type(config) is type({})
    assert len(config.items()) == 0


def test_config_dict_copied_not_ref():
    """Test that config holder config can't be set from outside"""
    config = ConfigHolder.get_config()

    config["test"] = 123

    assert "test" not in ConfigHolder.get_config()


def test_config():
    ConfigHolder.set_custom_config(hello=123)
    ConfigHolder.set_custom_config(world="test")

    config = ConfigHolder.get_config()

    assert config["hello"] == 123
    assert config["world"] == "test"


def test_config_from_dict():
    ConfigHolder.set_custom_config(**{"dict_item_1": 123, "dict_item_2": 456})

    config = ConfigHolder.get_config()

    assert config["dict_item_1"] == 123
    assert config["dict_item_2"] == 456


def test_dependencies_unset():
    deps = ConfigHolder.get_dependency_config()
    assert len(deps.keys()) == 0


def test_dependencies():
    ConfigHolder.set_dependency_config({"hello": 123})

    deps = ConfigHolder.get_dependency_config()

    assert deps["hello"] == 123
