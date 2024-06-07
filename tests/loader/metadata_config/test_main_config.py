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

import pytest
from pydantic import ValidationError

from rialto.loader.config_loader import get_feature_config


def test_get_config_full_cfg():
    cfg = get_feature_config("tests/loader/metadata_config/full_example.yaml")
    assert len(cfg.selection) == 2
    assert cfg.selection[0].group == "A"
    assert cfg.selection[0].prefix == "A"
    assert cfg.selection[0].features == ["A1", "A2"]
    assert cfg.selection[1].group == "B"
    assert cfg.selection[1].prefix == "B"
    assert cfg.selection[1].features == ["B1", "B2"]
    assert cfg.base.group == "D"
    assert cfg.base.keys == ["K", "L"]
    assert cfg.maps == ["M", "N"]


def test_get_config_no_map_cfg():
    cfg = get_feature_config("tests/loader/metadata_config/no_map_example.yaml")
    assert len(cfg.selection) == 2
    assert cfg.selection[0].group == "A"
    assert cfg.selection[0].prefix == "A"
    assert cfg.selection[0].features == ["A1", "A2"]
    assert cfg.selection[1].group == "B"
    assert cfg.selection[1].prefix == "B"
    assert cfg.selection[1].features == ["B1", "B2"]
    assert cfg.base.group == "D"
    assert cfg.base.keys == ["K", "L"]
    assert cfg.maps is None


def test_get_config_no_base_key():
    with pytest.raises(ValidationError):
        get_feature_config("tests/loader/metadata_config/missing_value_example.yaml")


def test_get_config_no_prefix_field():
    with pytest.raises(ValidationError):
        get_feature_config("tests/loader/metadata_config/missing_field_example.yaml")
