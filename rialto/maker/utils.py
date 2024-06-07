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

__all__ = ["feature_name"]

import typing


def _get_feature_parameter_suffix(parameters: typing.Dict) -> str:
    """
    Collate all parameter names and their value in format p1_v1_p2_v2...

    :param parameters: unordered dict of parameters and values
    :return: string serialized parameters
    """
    if len(parameters) == 0:
        return ""
    params = ""
    for parameter_name, value in sorted(parameters.items()):
        params += f"_{parameter_name}_{value}"
    return params


def feature_name(name: str, parameters: typing.Dict) -> str:
    """
    Join feature function name with parameters to create a feature name

    :param name: string name of feature
    :param parameters: unordered dict of parameters and values
    :return: feature name
    """
    params = _get_feature_parameter_suffix(parameters)
    if len(params) == 0:
        return name.upper()
    return f"{name}{params}".upper()
