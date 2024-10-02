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

__all__ = ["FeatureMaker"]

import datetime
import inspect
import types
import typing

import pyspark.sql.functions as F
from loguru import logger
from pyspark.sql import DataFrame

from rialto.maker.containers import FeatureFunction, FeatureHolder


class _FeatureMaker:
    """
    A framework for feature making

    Enables registration of callable feature functions and then executes them.
    """

    def __init__(self):
        self.feature_functions = FeatureHolder()
        self.data_frame = None
        self.make_date = None
        self.key = None

    def _set_values(self, df: DataFrame, key: typing.Union[str, typing.List[str]], make_date: datetime.date):
        """
        Instance value setter

        :param df: DataFrame with input data
        :param key: simple or compound string key
        :param make_date: a date to make feature from
        :return: None
        """
        self.data_frame = df
        if isinstance(key, str):
            self.key = [key]
        else:
            self.key = key
        self.make_date = make_date

    def _order_by_dependencies(self, feature_holders: typing.List[FeatureHolder]) -> typing.List[FeatureHolder]:
        """
        Order features like directional graph

        Simple O(n^2) solution, in each pass try to find one feature group with dependencies resolved
        :param feature_holders: List of feature holders each with one group of feature_functions
        :return: ordered list of feature holders each with one group of feature_functions
        """
        logger.trace("Resolving module dependencies")

        ordered = []
        resolved = set()
        leftover = feature_holders
        while True:
            dep_is_resolved = True
            i = 0
            for i in range(len(leftover)):
                dep_is_resolved = True
                for dep in leftover[i][0].dependencies:
                    if dep not in resolved:
                        dep_is_resolved = False
                        break
                if dep_is_resolved:
                    break
            if dep_is_resolved:
                resolved.add(leftover[i][0].name)
                ordered.append(leftover.pop(i))
            else:
                raise Exception("Feature dependencies can't be resolved!")

            if len(leftover) == 0:
                break
        return ordered

    def _load_features(self, features_module: types.ModuleType) -> typing.List:
        """
        Find feature function definitions inside a given python module

        :param features_module: a python module with definitions of feature functions using wrappers
        :return: List of FeatureHolders
        """
        return [value for (member, value) in inspect.getmembers(features_module) if isinstance(value, FeatureHolder)]

    def _register_module(self, features_module: types.ModuleType) -> FeatureHolder:
        """
        Find feature function definitions inside a given python module and registers them for use in make call

        :param features_module: a python module with definitions of feature functions using FML wrappers
        :return: FeatureHolder
        """
        logger.trace(f"Registering module: {features_module}")
        feature_functions = FeatureHolder()

        ordered_feature_holders = self._order_by_dependencies(self._load_features(features_module))
        for feature_holder in ordered_feature_holders:
            for feature_function in feature_holder:
                logger.trace(f"Registering feature:\n\t{feature_function}")
                feature_functions.append(feature_function)
        return feature_functions

    def _filter_null_keys(self, df: DataFrame):
        if isinstance(self.key, str):
            return df.filter(F.col(self.key).isNotNull())
        else:
            for k in self.key:
                df = df.filter(F.col(k).isNotNull())
        return df

    def _make_sequential(self, keep_preexisting: bool) -> DataFrame:
        """
        Make features by creating new columns on the existing dataframe

        :param keep_preexisting: Bool, keep preexisting data in the dataframe after making features
        :return: DataFrame
        """
        feature_names = []
        for feature_function in self.feature_functions:
            logger.trace(f"Making feature:\n\t{feature_function}")
            feature_names.append(feature_function.get_feature_name())
            self.data_frame = self.data_frame.withColumn(
                feature_function.get_feature_name(), feature_function.callable()
            )
        if not keep_preexisting:
            logger.info("Dropping non-selected columns")
            self.data_frame = self.data_frame.select(*self.key, *feature_names)
        return self._filter_null_keys(self.data_frame)

    def _make_aggregated(self) -> DataFrame:
        """
        Make features by creating new columns via aggregation on defined key

        :return: DataFrame
        """
        aggregates = []
        for feature_function in self.feature_functions:
            logger.trace(f"Creating aggregate: \n\t{feature_function.__str__()}")
            aggregates.append(feature_function.callable().alias(feature_function.get_feature_name()))

        self.data_frame = self.data_frame.groupBy(self.key).agg(*aggregates)
        return self._filter_null_keys(self.data_frame)

    def make(
        self,
        df: DataFrame,
        key: typing.Union[str, typing.List[str]],
        make_date: datetime.date,
        features_module: types.ModuleType,
        keep_preexisting: bool = False,
    ) -> (DataFrame, typing.Dict):
        """
        Make features by creating new columns based on definitions in imported module

        :param df: DataFrame with input data
        :param key: simple or compound string key
        :param make_date: a date to make feature from
        :param features_module: a python module with definitions of feature functions using FML wrappers
        :param keep_preexisting: bool to decide whether to keep input data
        :return: DataFrame with features, Dict with feature metadata
        """
        logger.info(f"Making sequential for \n\tkey: {key} \n\ton {make_date} \n\tfrom {features_module}")
        self._set_values(df, key, make_date)
        self.feature_functions = self._register_module(features_module)
        features = self._make_sequential(keep_preexisting)
        logger.info(f"Finished making sequential features from {features_module}")
        return features, self.feature_functions.get_metadata()

    def make_aggregated(
        self,
        df: DataFrame,
        key: typing.Union[str, typing.List[str]],
        make_date: datetime.date,
        features_module: types.ModuleType,
    ) -> (DataFrame, typing.Dict):
        """
        Make features by creating new columns based on definitions in imported module

        :param df: DataFrame with input data
        :param key: simple or compound string key
        :param make_date: a date to make feature from
        :param features_module: a python module with definitions of feature functions using FML wrappers
        :return: DataFrame with features, Dict with feature metadata
        """
        logger.info(f"Making aggregated for \n\tkey: {key} \n\ton {make_date} \n\tfrom {features_module}")
        self._set_values(df, key, make_date)
        self.feature_functions = self._register_module(features_module)
        features = self._make_aggregated()
        logger.info(f"Finished making aggregated features from {features_module}")
        return features, self.feature_functions.get_metadata()

    def _find_feature(self, name: str, feature_functions: FeatureHolder) -> FeatureFunction:
        """
        Find a single feature function by name

        :param name: str name of the feature
        :param feature_functions: FeatureHolder with all feature functions
        :return: wanted FeatureFunction
        """
        for feature_function in feature_functions:
            if feature_function.get_feature_name() == name:
                return feature_function
        raise Exception(f"Feature {name} is not defined!")

    def make_single_feature(
        self,
        df: DataFrame,
        name: typing.Union[str, typing.List[str]],
        features_module: types.ModuleType,
        make_date: datetime.date = None,
    ) -> DataFrame:
        """
        Make single feature by creating new columns based on definition in imported module

        Intended for being able to test single features
        :param df: DataFrame with input data
        :param name: name of the feature
        :param features_module: a python module with definitions of feature functions using FML wrappers
        :param make_date: an optional date to make feature from
        :return: DataFrame with features
        """
        self.make_date = make_date
        feature_functions = self._register_module(features_module)
        feature = self._find_feature(name, feature_functions)
        return df.withColumn(feature.get_feature_name(), feature.callable()).select(feature.get_feature_name())

    def make_single_agg_feature(
        self,
        df: DataFrame,
        name: str,
        key: typing.Union[str, typing.List[str]],
        features_module: types.ModuleType,
        make_date: datetime.date = None,
    ) -> DataFrame:
        """
        Make single feature by creating new columns based on definition in imported module

        Intended for being able to test single features
        :param df: DataFrame with input data
        :param name: name of the feature
        :param key: key to aggregate on
        :param features_module: a python module with definitions of feature functions using FML wrappers
        :param make_date: an optional date to make feature from
        :return: DataFrame with features
        """
        self.make_date = make_date
        feature_functions = self._register_module(features_module)
        feature = self._find_feature(name, feature_functions)
        return df.groupBy(key).agg(feature.callable().alias(feature.get_feature_name()))


FeatureMaker = _FeatureMaker()
