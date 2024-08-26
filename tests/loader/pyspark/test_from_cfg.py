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
from unittest.mock import MagicMock

import pytest
from chispa import assert_df_equality
from pyspark.sql import SparkSession

import tests.loader.pyspark.resources as r
from rialto.loader.config_loader import get_feature_config
from rialto.loader.pyspark_feature_loader import PysparkFeatureLoader
from tests.loader.pyspark.dataframe_builder import dataframe_builder as dfb


@pytest.fixture(scope="session")
def spark(request):
    """fixture for creating a spark session
    :param request: pytest.FixtureRequest object
    """
    spark = (
        SparkSession.builder.master("local[2]")
        .appName("pytest-pyspark-local-testing")
        .config("spark.ui.enabled", "false")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.driver.host", "127.0.0.1")
        .getOrCreate()
    )

    request.addfinalizer(lambda: spark.stop())

    return spark


@pytest.fixture(scope="session")
def loader(spark):
    return PysparkFeatureLoader(spark, MagicMock(), MagicMock())


VALID_LIST = [(["a"], ["a"]), (["a"], ["a", "b", "c"]), (["c", "a"], ["a", "b", "c"])]


@pytest.mark.parametrize("valid_terms", VALID_LIST)
def test_all_keys_in_true(loader, valid_terms):
    assert loader._are_all_keys_in(valid_terms[0], valid_terms[1]) is True


INVALID_LIST = [(["d"], ["a"]), (["a", "d"], ["a", "b", "c"]), (["c", "a", "b"], ["a", "c"])]


@pytest.mark.parametrize("invalid_terms", INVALID_LIST)
def test_all_keys_in_false(loader, invalid_terms):
    assert loader._are_all_keys_in(invalid_terms[0], invalid_terms[1]) is False


def test_add_prefix(loader):
    df = dfb(loader.spark, data=r.feature_group_a_data, columns=r.feature_group_a_columns)
    assert loader._add_prefix(df, "A", ["KEY"]).columns == ["KEY", "A_A1", "A_A2"]


def test_join_keymaps(loader, spark):
    key_maps = [
        PysparkFeatureLoader.KeyMap(dfb(spark, data=r.mapping1_data, columns=r.mapping1_columns), ["KEY1", "KEY2"]),
        PysparkFeatureLoader.KeyMap(dfb(spark, data=r.mapping2_data, columns=r.mapping2_columns), ["KEY1"]),
        PysparkFeatureLoader.KeyMap(dfb(spark, data=r.mapping3_data, columns=r.mapping3_columns), ["KEY3"]),
    ]
    mapped = loader._join_keymaps(dfb(spark, data=r.base_frame_data, columns=r.base_frame_columns), key_maps)
    expected = dfb(spark, data=r.expected_mapping_data, columns=r.expected_mapping_columns)
    assert_df_equality(mapped, expected, ignore_column_order=True, ignore_row_order=True)


def test_add_group(spark, monkeypatch):
    class GroupMd:
        def __init__(self):
            self.key = ["KEY1"]

        def __call__(self, *args, **kwargs):
            return self

    metadata = MagicMock()
    monkeypatch.setattr(metadata, "get_group", GroupMd())
    loader = PysparkFeatureLoader(spark, "", "")
    loader.metadata = metadata

    base = dfb(spark, data=r.base_frame_data, columns=r.base_frame_columns)
    df = dfb(spark, data=r.feature_group_b_data, columns=r.feature_group_b_columns)
    group_cfg = get_feature_config("tests/loader/pyspark/example_cfg.yaml").selection[0]

    features = loader._add_feature_group(base, df, group_cfg)
    expected = dfb(spark, data=r.expected_features_b_data, columns=r.expected_features_b_columns)
    assert_df_equality(features, expected, ignore_column_order=True, ignore_row_order=True)


def test_get_group_metadata(spark, mocker):
    mocker.patch("rialto.loader.pyspark_feature_loader.MetadataManager.get_group", return_value=7)

    loader = PysparkFeatureLoader(spark, "", "")
    ret_val = loader.get_group_metadata("group_name")

    assert ret_val == 7
    loader.metadata.get_group.assert_called_once_with("group_name")


def test_get_feature_metadata(spark, mocker):
    mocker.patch("rialto.loader.pyspark_feature_loader.MetadataManager.get_feature", return_value=8)

    loader = PysparkFeatureLoader(spark, "", "")
    ret_val = loader.get_feature_metadata("group_name", "feature")

    assert ret_val == 8
    loader.metadata.get_feature.assert_called_once_with("group_name", "feature")


def test_get_metadata_from_cfg(spark, mocker):
    mocker.patch(
        "rialto.loader.pyspark_feature_loader.MetadataManager.get_feature",
        side_effect=lambda g, f: {"B": {"F1": 1, "F3": 2}}[g][f],
    )
    mocker.patch("rialto.loader.pyspark_feature_loader.MetadataManager.get_group", side_effect=lambda g: {"B": 10}[g])

    loader = PysparkFeatureLoader(spark, "", "")
    metadata = loader.get_metadata_from_cfg("tests/loader/pyspark/example_cfg.yaml")

    assert metadata["B_F1"] == 1
    assert metadata["B_F3"] == 2
    assert len(metadata.keys()) == 2
