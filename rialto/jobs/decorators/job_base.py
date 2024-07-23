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
from contextlib import contextmanager

import pyspark.sql.functions as F
from loguru import logger
from pyspark.sql import DataFrame, SparkSession

from rialto.common import TableReader
from rialto.jobs.configuration.config_holder import ConfigHolder
from rialto.jobs.decorators.resolver import Resolver
from rialto.loader import DatabricksLoader, PysparkFeatureLoader
from rialto.metadata import MetadataManager
from rialto.runner import Transformation


class JobBase(Transformation):
    """A Base Class for Rialto Jobs. Serves as a foundation into which the @job decorators are inserted."""

    @abc.abstractmethod
    def get_custom_callable(self) -> typing.Callable:
        """Getter - Custom callable (i.e. job transformation function)"""
        pass

    @abc.abstractmethod
    def get_job_version(self) -> str:
        """Job version getter"""
        pass

    @abc.abstractmethod
    def get_job_name(self) -> str:
        """Job name getter"""
        pass

    @contextmanager
    def _setup_resolver(self, run_date: datetime.date) -> None:
        Resolver.register_callable(lambda: run_date, "run_date")

        Resolver.register_callable(ConfigHolder.get_config, "config")
        Resolver.register_callable(ConfigHolder.get_dependency_config, "dependencies")

        Resolver.register_callable(self._get_spark, "spark")
        Resolver.register_callable(self._get_table_reader, "table_reader")
        Resolver.register_callable(self._get_feature_loader, "feature_loader")

        try:
            yield
        finally:
            Resolver.cache_clear()

    def _setup(
        self, spark: SparkSession, run_date: datetime.date, table_reader: TableReader, dependencies: typing.Dict = None
    ) -> None:
        self._spark = spark
        self._table_rader = table_reader

        ConfigHolder.set_dependency_config(dependencies)
        ConfigHolder.set_run_date(run_date)

    def _get_spark(self) -> SparkSession:
        return self._spark

    def _get_table_reader(self) -> TableReader:
        return self._table_rader

    def _get_feature_loader(self) -> PysparkFeatureLoader:
        config = ConfigHolder.get_feature_store_config()

        databricks_loader = DatabricksLoader(self._spark, config.feature_store_schema)
        feature_loader = PysparkFeatureLoader(self._spark, databricks_loader, config.feature_metadata_schema)

        return feature_loader

    def _get_timestamp_holder_result(self) -> DataFrame:
        spark = self._get_spark()
        return spark.createDataFrame(
            [(self.get_job_name(), datetime.datetime.now())], schema="JOB_NAME string, CREATION_TIME timestamp"
        )

    def _add_job_version(self, df: DataFrame) -> DataFrame:
        version = self.get_job_version()

        if version is not None:
            return df.withColumn("VERSION", F.lit(version))

        return df

    def _run_main_callable(self, run_date: datetime.date) -> DataFrame:
        with self._setup_resolver(run_date):
            custom_callable = self.get_custom_callable()
            raw_result = Resolver.register_resolve(custom_callable)

        if raw_result is None:
            raw_result = self._get_timestamp_holder_result()

        result_with_version = self._add_job_version(raw_result)
        return result_with_version

    def run(
        self,
        reader: TableReader,
        run_date: datetime.date,
        spark: SparkSession = None,
        metadata_manager: MetadataManager = None,
        dependencies: typing.Dict = None,
    ) -> DataFrame:
        """
        Rialto transformation run

        :param reader: data store api object
        :param info_date: date
        :param spark: spark session
        :param metadata_manager: metadata api object
        :param dependencies: rialto job dependencies
        :return: dataframe
        """
        try:
            self._setup(spark, run_date, reader, dependencies)
            return self._run_main_callable(run_date)
        except Exception as e:
            logger.exception(e)
            raise e
