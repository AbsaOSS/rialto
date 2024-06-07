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
from datetime import datetime
from typing import Dict

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType

from rialto.common import TableReader
from rialto.metadata import MetadataManager
from rialto.runner import Transformation


class SimpleGroup(Transformation):
    def run(
        self,
        reader: TableReader,
        run_date: datetime.date,
        spark: SparkSession = None,
        metadata_manager: MetadataManager = None,
        dependencies: Dict = None,
    ) -> DataFrame:
        return spark.createDataFrame([], StructType([]))
