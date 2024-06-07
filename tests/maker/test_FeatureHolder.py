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

from rialto.maker.containers import FeatureFunction, FeatureHolder
from rialto.metadata import ValueType


def test_metadata_return_type_empty():
    assert isinstance(FeatureHolder().get_metadata(), list)


def test_metadata_return_type():
    fh = FeatureHolder()
    fh.append(FeatureFunction("feature_nominal", Mock(), ValueType.nominal))
    assert isinstance(fh.get_metadata(), list)


def test_metadata_value():
    fh = FeatureHolder()
    ff = FeatureFunction("feature_ordinal", Mock(), ValueType.ordinal)
    ff.parameters["param"] = 3
    fh.append(ff)
    metadata = fh.get_metadata()
    assert metadata[0].value_type == ValueType.ordinal
