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
from unittest.mock import Mock

from rialto.runner.services.config_loader import TargetConfig
from rialto.runner.services.table import Table


def test_table_basic_init():
    t = Table(catalog="cat", schema="sch", table="tab", schema_path=None, table_path=None, class_name=None)

    assert t.get_table_path() == "cat.sch.tab"
    assert t.get_schema_path() == "cat.sch"


def test_table_classname_init():
    t = Table(catalog=None, schema=None, table=None, schema_path="cat.sch", table_path=None, class_name="ClaSs")

    assert t.get_table_path() == "cat.sch.cla_ss"
    assert t.get_schema_path() == "cat.sch"
    assert t.catalog == "cat"
    assert t.schema == "sch"
    assert t.table == "cla_ss"


def test_table_path_init():
    t = Table(catalog=None, schema=None, table=None, schema_path=None, table_path="cat.sch.tab", class_name=None)

    assert t.get_table_path() == "cat.sch.tab"
    assert t.get_schema_path() == "cat.sch"
    assert t.catalog == "cat"
    assert t.schema == "sch"
    assert t.table == "tab"


def test_table_secondary_partitions():
    t = Table(catalog="cat", schema="sch", table="tab", partition="part", secondary_partitions=["sec1", "sec2"])

    assert t.get_all_partition_columns() == ["part", "sec1", "sec2"]


def test_table_get_partitions_only_main():
    t = Table(catalog="cat", schema="sch", table="tab", partition="part")

    assert t.get_all_partition_columns() == ["part"]


def test_table_prioritize_table_name():
    t = Table(catalog=None, schema=None, table="custom", schema_path="cat.sch", table_path=None, class_name="ClaSs")

    assert t.get_table_path() == "cat.sch.custom"
    assert t.get_schema_path() == "cat.sch"
    assert t.catalog == "cat"
    assert t.schema == "sch"
    assert t.table == "custom"


def test_from_target_config():
    tconfig = TargetConfig(
        target_schema="cat.sch",
        target_partition_column="part",
        secondary_partition_columns=["sec1", "sec2"],
        custom_name=None,
        rerun_filters={"col": "value"},
    )

    pipeline_cfg = Mock()
    pipeline_cfg.module.python_class = "TestClass"
    pipeline_cfg.target = tconfig

    t = Table.from_target_config(pipeline_cfg)

    assert t.get_table_path() == "cat.sch.test_class"
    assert t.get_schema_path() == "cat.sch"
    assert t.catalog == "cat"
    assert t.schema == "sch"
    assert t.table == "test_class"
    assert t.get_all_partition_columns() == ["part", "sec1", "sec2"]
    assert t.filters == {"col": "value"}


def test_from_dependency_config():
    dconfig = Mock()
    dconfig.table = "cat.sch.tab"
    dconfig.date_col = "date"
    dconfig.filters = {"col": "value"}

    t = Table.from_dependency_config(dconfig)

    assert t.get_table_path() == "cat.sch.tab"
    assert t.get_schema_path() == "cat.sch"
    assert t.catalog == "cat"
    assert t.schema == "sch"
    assert t.table == "tab"
    assert t.partition == "date"
    assert t.filters == {"col": "value"}
