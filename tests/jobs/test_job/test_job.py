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


from rialto.jobs.decorators import datasource, job


@datasource
def dataset():
    return "dataset_return"


@job
def job_function():
    return "job_function_return"


@job("custom_job_name")
def custom_name_job_function():
    return "custom_job_name_return"


@job
def job_asking_for_all_deps(spark, run_date, config, dependencies, table_reader):
    assert spark is not None
    assert run_date == 456
    assert config["value"] == 123
    assert table_reader == 789
    assert dependencies == 1011
