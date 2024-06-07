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

import typing

from pyspark.sql.types import DataType, StructField, StructType


def dataframe_builder(
    spark, data: typing.List, columns: typing.List[typing.Union[typing.Tuple[str, typing.Type[DataType]]]]
):
    schema_builder = []
    for name, data_type in columns:
        schema_builder.append(StructField(name, data_type, True))
    schema = StructType(schema_builder)
    return spark.createDataFrame(data, schema)
