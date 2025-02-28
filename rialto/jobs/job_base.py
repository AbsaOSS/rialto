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

__all__ = ["JobBase"]

import abc
import datetime
import typing

import pyspark.sql.functions as F
from loguru import logger
from pydantic import BaseModel
from pyspark.sql import DataFrame, SparkSession

from rialto.common import TableReader
from rialto.jobs.resolver import Resolver
from rialto.loader import PysparkFeatureLoader
from rialto.metadata import MetadataManager
from rialto.runner import Transformation
from rialto.runner.config_loader import PipelineConfig


class JobMetadata(BaseModel):
    job_name: str
    dist_name: str
    dist_version: str


class JobBase(Transformation):
    """A Base Class for Rialto Jobs. Serves as a foundation into which the @job decorators are inserted."""

    @abc.abstractmethod
    def get_custom_callable(self) -> typing.Callable:
        """Getter - Custom callable (i.e. job transformation function)"""
        pass

    @abc.abstractmethod
    def get_job_metadata(self) -> JobMetadata:
        """Job metadata getter"""
        pass

    @abc.abstractmethod
    def get_job_name(self) -> str:
        """Job name getter"""
        pass

    @abc.abstractmethod
    def get_disable_version(self) -> bool:
        """Disable version getter"""
        pass

    def _get_resolver(
        self,
        spark: SparkSession,
        run_date: datetime.date,
        table_reader: TableReader,
        config: PipelineConfig = None,
        metadata_manager: MetadataManager = None,
        feature_loader: PysparkFeatureLoader = None,
    ) -> Resolver:
        resolver = Resolver()

        # Static Always - Available dependencies
        resolver.register_object(spark, "spark")
        resolver.register_object(run_date, "run_date")
        resolver.register_object(config, "config")
        resolver.register_object(self.get_job_metadata(), "job_metadata")
        resolver.register_object(table_reader, "table_reader")

        # Optionals
        if feature_loader is not None:
            resolver.register_object(feature_loader, "feature_loader")

        if metadata_manager is not None:
            resolver.register_object(metadata_manager, "metadata_manager")

        return resolver

    def _get_timestamp_holder_result(self, spark) -> DataFrame:
        return spark.createDataFrame(
            [(self.get_job_name(), datetime.datetime.now())], schema="JOB_NAME string, CREATION_TIME timestamp"
        )

    def _add_job_version(self, df: DataFrame) -> DataFrame:
        version = self.get_job_metadata().dist_version
        return df.withColumn("VERSION", F.lit(version))

    def run(
        self,
        reader: TableReader,
        run_date: datetime.date,
        spark: SparkSession = None,
        config: PipelineConfig = None,
        metadata_manager: MetadataManager = None,
        feature_loader: PysparkFeatureLoader = None,
    ) -> DataFrame:
        """
        Rialto transformation run

        :param reader: data store api object
        :param info_date: date
        :param spark: spark session
        :param config: pipeline config
        :return: dataframe
        """
        try:
            resolver = self._get_resolver(spark, run_date, reader, config, metadata_manager, feature_loader)

            custom_callable = self.get_custom_callable()
            raw_result = resolver.resolve(custom_callable)

            if raw_result is None:
                raw_result = self._get_timestamp_holder_result(spark)

            if not self.get_disable_version():
                return self._add_job_version(raw_result)

            return raw_result

        except Exception as e:
            logger.exception(e)
            raise e
