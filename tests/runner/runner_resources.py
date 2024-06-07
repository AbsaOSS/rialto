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
from pyspark.sql.types import DateType, StringType, StructField, StructType

from rialto.runner.date_manager import DateManager

simple_group_data = [
    ("A", DateManager.str_to_date("2023-03-05")),
    ("B", DateManager.str_to_date("2023-03-12")),
    ("C", DateManager.str_to_date("2023-03-19")),
]

general_schema = StructType([StructField("KEY", StringType(), True), StructField("DATE", DateType(), True)])


dep1_data = [
    ("E", DateManager.str_to_date("2023-03-05")),
    ("F", DateManager.str_to_date("2023-03-10")),
    ("G", DateManager.str_to_date("2023-03-15")),
    ("H", DateManager.str_to_date("2023-03-25")),
]

dep2_data = [
    ("J", DateManager.str_to_date("2022-11-01")),
    ("K", DateManager.str_to_date("2022-12-01")),
    ("L", DateManager.str_to_date("2023-01-01")),
]
