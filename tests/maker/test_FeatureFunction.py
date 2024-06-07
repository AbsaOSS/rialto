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
from unittest.mock import Mock

import pytest

from rialto.maker.containers import FeatureFunction
from rialto.metadata import ValueType


def test_name_generation_no_parameters():
    func = FeatureFunction("feature", Mock())
    assert func.get_feature_name() == "FEATURE"


def test_name_generation_with_parameter():
    func = FeatureFunction("feature", Mock())
    func.parameters["param"] = 6
    assert func.get_feature_name() == "FEATURE_PARAM_6"


def test_name_generation_multiple_params():
    func = FeatureFunction("feature", Mock())
    func.parameters["paramC"] = 1
    func.parameters["paramA"] = 4
    func.parameters["paramB"] = 6
    assert func.get_feature_name() == "FEATURE_PARAMA_4_PARAMB_6_PARAMC_1"


def test_feature_type_default_is_nominal():
    func = FeatureFunction("feature", Mock())
    assert func.type == ValueType.nominal


@pytest.mark.parametrize(
    "feature_type",
    [(ValueType.nominal, "nominal"), (ValueType.ordinal, "ordinal"), (ValueType.numerical, "numerical")],
)
def test_feature_type_getter(feature_type: tuple):
    func = FeatureFunction("feature", Mock(), feature_type[0])
    assert func.get_type() == feature_type[1]


def test_serialization():
    func = FeatureFunction("feature", Mock())
    func.parameters["paramC"] = 1
    func.parameters["paramA"] = 4
    assert (
        func.__str__()
        == "Name: feature\n\tParameters: {'paramC': 1, 'paramA': 4}\n\tType: nominal\n\tDescription: basic feature"
    )


def test_metadata():
    func = FeatureFunction("feature", Mock(), ValueType.ordinal)
    func.parameters["paramC"] = 1
    func.parameters["paramA"] = 4
    func.dependencies = ["featureB", "featureC"]
    func.description = "nice feature"

    assert func.metadata().name == "FEATURE_PARAMA_4_PARAMC_1"
    assert func.metadata().value_type == ValueType.ordinal
    assert func.metadata().description == "nice feature"
