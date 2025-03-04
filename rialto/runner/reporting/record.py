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

__all__ = ["Record"]

from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Optional

from pyspark.sql import Row
from pyspark.sql.types import (
    DateType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)


@dataclass
class Record:
    """Dataclass with information about one run of one pipeline."""

    job: str
    target: str
    date: datetime.date
    time: timedelta
    records: int
    status: str
    reason: str
    exception: Optional[str] = None
    run_timestamp: datetime.timestamp = datetime.now()

    def get_schema(self) -> StructType:
        """Retrieve schema of pyspark DataFrame"""
        return StructType(
            [
                StructField("job", StringType(), nullable=False),
                StructField("target", StringType(), nullable=False),
                StructField("date", DateType(), nullable=False),
                StructField("time", StringType(), nullable=False),
                StructField("records", IntegerType(), nullable=False),
                StructField("status", StringType(), nullable=False),
                StructField("reason", StringType(), nullable=False),
                StructField("exception", StringType(), nullable=True),
                StructField("run_timestamp", TimestampType(), nullable=False),
            ]
        )

    def to_spark_row(self) -> Row:
        """Convert Record to Spark Row"""
        return Row(
            job=self.job,
            target=self.target,
            date=self.date,
            time=str(self.time),
            records=self.records,
            status=self.status,
            reason=self.reason,
            exception=self.exception,
            run_timestamp=self.run_timestamp,
        )
