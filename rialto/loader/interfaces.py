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

__all__ = ["FeatureLoaderInterface"]

import abc
from datetime import date
from typing import Dict


class FeatureLoaderInterface(metaclass=abc.ABCMeta):
    """
    A definition of feature loading interface

    Provides functionality to read features, feature groups and selections of features according to configs.
    Also provides an interface to access metadata of said features.
    """

    @abc.abstractmethod
    def get_feature(self, group_name: str, feature_name: str, information_date: date):
        """Get single feature"""
        raise NotImplementedError

    @abc.abstractmethod
    def get_feature_metadata(self, group_name: str, feature_name: str) -> Dict:
        """Get single feature's metadata"""
        raise NotImplementedError

    @abc.abstractmethod
    def get_group(self, group_name: str, information_date: date):
        """Get feature group"""
        raise NotImplementedError

    @abc.abstractmethod
    def get_group_metadata(self, group_name: str) -> Dict:
        """Get feature group's metadata"""
        raise NotImplementedError

    @abc.abstractmethod
    def get_features_from_cfg(self, path: str, information_date: date):
        """Get features from multiple groups as defined by configuration"""
        raise NotImplementedError

    @abc.abstractmethod
    def get_metadata_from_cfg(self, path: str) -> Dict:
        """Get metadata from multiple groups as defined by configuration"""
        raise NotImplementedError
