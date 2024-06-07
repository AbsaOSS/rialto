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

__all__ = ["ConfigException", "FeatureStoreConfig", "ConfigHolder"]

import datetime
import typing

from pydantic import BaseModel


class ConfigException(Exception):
    """Wrong Configuration Exception"""

    pass


class FeatureStoreConfig(BaseModel):
    """Configuration of Feature Store Paths"""

    feature_store_schema: str = None
    feature_metadata_schema: str = None


class ConfigHolder:
    """
    Main Rialto Jobs config holder.

    Configured via job_runner and then called from job_base / job decorators.
    """

    _config = {}
    _dependencies = {}
    _run_date = None
    _feature_store_config: FeatureStoreConfig = None

    @classmethod
    def set_run_date(cls, run_date: datetime.date) -> None:
        """
        Inicialize run Date

        :param run_date: datetime.date, run date
        :return: None
        """
        cls._run_date = run_date

    @classmethod
    def get_run_date(cls) -> datetime.date:
        """
        Run date

        :return: datetime.date, Run date
        """
        if cls._run_date is None:
            raise ConfigException("Run Date not Set !")
        return cls._run_date

    @classmethod
    def set_feature_store_config(cls, feature_store_schema: str, feature_metadata_schema: str) -> None:
        """
        Inicialize feature store config

        :param feature_store_schema: str, schema name
        :param feature_metadata_schema: str, metadata schema name
        :return: None
        """
        cls._feature_store_config = FeatureStoreConfig(
            feature_store_schema=feature_store_schema, feature_metadata_schema=feature_metadata_schema
        )

    @classmethod
    def get_feature_store_config(cls) -> FeatureStoreConfig:
        """
        Feature Store Config

        :return: FeatureStoreConfig
        """
        if cls._feature_store_config is None:
            raise ConfigException("Feature Store Config not Set !")

        return cls._feature_store_config

    @classmethod
    def get_config(cls) -> typing.Dict:
        """
        Get config dictionary

        :return: dictionary of key-value pairs
        """
        return cls._config.copy()

    @classmethod
    def set_custom_config(cls, **kwargs) -> None:
        """
        Set custom key-value pairs for custom config

        :param kwargs: key-value pairs to setup
        :return: None
        """
        cls._config.update(kwargs)

    @classmethod
    def get_dependency_config(cls) -> typing.Dict:
        """
        Get rialto job dependency config

        :return: dictionary with dependency config
        """
        return cls._dependencies

    @classmethod
    def set_dependency_config(cls, dependencies: typing.Dict) -> None:
        """
        Get rialto job dependency config

        :param dependencies: dictionary with the config
        :return: None
        """
        cls._dependencies = dependencies
