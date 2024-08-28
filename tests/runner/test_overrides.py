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

from rialto.runner import Runner


def test_overrides_simple(spark):
    runner = Runner(
        spark,
        config_path="tests/runner/overrider.yaml",
        run_date="2023-03-31",
        overrides={"runner.mail.to": ["x@b.c", "y@b.c", "z@b.c"]},
    )
    assert runner.config.runner.mail.to == ["x@b.c", "y@b.c", "z@b.c"]


def test_overrides_array_index(spark):
    runner = Runner(
        spark,
        config_path="tests/runner/overrider.yaml",
        run_date="2023-03-31",
        overrides={"runner.mail.to[1]": "a@b.c"},
    )
    assert runner.config.runner.mail.to == ["developer@testing.org", "a@b.c"]


def test_overrides_array_append(spark):
    runner = Runner(
        spark,
        config_path="tests/runner/overrider.yaml",
        run_date="2023-03-31",
        overrides={"runner.mail.to[-1]": "test"},
    )
    assert runner.config.runner.mail.to == ["developer@testing.org", "developer2@testing.org", "test"]


def test_overrides_array_lookup(spark):
    runner = Runner(
        spark,
        config_path="tests/runner/overrider.yaml",
        run_date="2023-03-31",
        overrides={"pipelines[name=SimpleGroup].target.target_schema": "new_schema"},
    )
    assert runner.config.pipelines[0].target.target_schema == "new_schema"


def test_overrides_combined(spark):
    runner = Runner(
        spark,
        config_path="tests/runner/overrider.yaml",
        run_date="2023-03-31",
        overrides={
            "runner.mail.to": ["x@b.c", "y@b.c", "z@b.c"],
            "pipelines[name=SimpleGroup].target.target_schema": "new_schema",
            "pipelines[name=SimpleGroup].schedule.info_date_shift[0].value": 1,
        },
    )
    assert runner.config.runner.mail.to == ["x@b.c", "y@b.c", "z@b.c"]
    assert runner.config.pipelines[0].target.target_schema == "new_schema"
    assert runner.config.pipelines[0].schedule.info_date_shift[0].value == 1


def test_index_out_of_range(spark):
    with pytest.raises(IndexError) as error:
        Runner(
            spark,
            config_path="tests/runner/overrider.yaml",
            run_date="2023-03-31",
            overrides={"runner.mail.to[8]": "test"},
        )
    assert error.value.args[0] == "Index 8 out of bounds for key to[8]"


def test_invalid_index_key(spark):
    with pytest.raises(ValueError) as error:
        Runner(
            spark,
            config_path="tests/runner/overrider.yaml",
            run_date="2023-03-31",
            overrides={"runner.mail.test[8]": "test"},
        )
    assert error.value.args[0] == "Invalid key test"


def test_invalid_key(spark):
    with pytest.raises(ValueError) as error:
        Runner(
            spark,
            config_path="tests/runner/overrider.yaml",
            run_date="2023-03-31",
            overrides={"runner.mail.test": "test"},
        )
    assert error.value.args[0] == "Invalid key test"
