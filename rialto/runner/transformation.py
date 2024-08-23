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

__all__ = ["Transformation"]

import abc
import datetime
from typing import Dict

from pyspark.sql import DataFrame, SparkSession

from rialto.common import TableReader
from rialto.metadata import MetadataManager


class Transformation(metaclass=abc.ABCMeta):
    """Interface for feature implementation"""

    @abc.abstractmethod
    def run(
        self,
        reader: TableReader,
        run_date: datetime.date,
        spark: SparkSession = None,
        metadata_manager: MetadataManager = None,
        dependencies: Dict = None,
    ) -> DataFrame:
        """
        Run the transformation

        :param reader: data store api object
        :param run_date: date
        :param spark: spark session
        :param metadata_manager: metadata api object
        :param dependencies: dictionary of dependencies
        :return: dataframe
        """
        raise NotImplementedError
