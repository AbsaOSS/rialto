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
from rialto.maker.containers import FeatureHolder
from rialto.maker.wrappers import depends, desc, feature, param
from rialto.metadata import ValueType


def dummy_feature_function():
    return None


def dummy_feature_with_args(parameter_1, parameter_2, parameter_3):
    return parameter_1 + parameter_2 + parameter_3


def test_feature_from_holder():
    val = feature(ValueType.numerical)(FeatureHolder())
    assert isinstance(val, FeatureHolder)


def test_feature_from_function_return_type():
    val = feature(ValueType.numerical)(dummy_feature_function)
    assert isinstance(val, FeatureHolder)


def test_feature_from_function_function_name():
    val = feature(ValueType.numerical)(dummy_feature_function)
    assert val[0].get_feature_name() == "DUMMY_FEATURE_FUNCTION"


def test_feature_from_function_function_object():
    val = feature(ValueType.numerical)(dummy_feature_function)
    assert val[0].callable == dummy_feature_function


def test_parametrize_from_function_return_type():
    val = param("parameter", [1, 2, 3])(dummy_feature_with_args)
    assert isinstance(val, FeatureHolder)


def test_parametrize_from_function_size():
    val = param("parameter", [1, 2, 3])(dummy_feature_with_args)
    assert len(val) == 3


def test_parametrize_chained_size():
    val = param("parameter_1", [1, 2, 3])(dummy_feature_with_args)
    val = param("parameter_2", [4, 5, 6])(val)
    assert len(val) == 9


def test_parametrize_chained_values():
    val = param("parameter_1", [1, 2, 3])(dummy_feature_with_args)
    val = param("parameter_2", [4, 5, 6])(val)
    val = param("parameter_3", [7, 8, 9])(val)
    # expecting ordered combinations (1,4,7)(1,4,8)(1,4,9)(1,5,7)(1,5,8).....
    assert (
        val[13].parameters["parameter_1"] == 2
        and val[13].parameters["parameter_2"] == 5
        and val[13].parameters["parameter_3"] == 8
    )


def test_parametrize_chained_callable():
    val = param("parameter_1", [1, 2, 3])(dummy_feature_with_args)
    val = param("parameter_2", [4, 5, 6])(val)
    val = param("parameter_3", [7, 8, 9])(val)
    assert val[13].callable() == 15


def test_feature_keeps_size():
    val = feature(ValueType.ordinal)(dummy_feature_function)
    assert len(val) == 1


def test_depends():
    val = depends("previous")(dummy_feature_function)
    assert val[0].dependencies[0] == "previous"


def test_depends_keeps_size():
    val = depends("previous")(dummy_feature_function)
    assert len(val) == 1


def test_description():
    val = desc("Feature A")(dummy_feature_function)
    assert val[0].description == "Feature A"


def test_description_keeps_size():
    val = desc("Feature A")(dummy_feature_function)
    assert len(val) == 1


def test_chaining():
    f = desc("Feature A")(dummy_feature_function)
    f = param("Param", ["B"])(f)
    f = depends("previous")(f)
    f = feature(ValueType.ordinal)(f)
    assert f[0].description == "Feature A"
    assert f[0].dependencies[0] == "previous"
    assert f[0].get_type() == "ordinal"
    assert f[0].get_feature_name() == "DUMMY_FEATURE_FUNCTION_PARAM_B"
    assert len(f) == 1
