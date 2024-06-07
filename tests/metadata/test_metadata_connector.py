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
import pytest

from tests.metadata.resources import feature_md1, group_md1, group_md2


def test_get_group_no_features(mdc):
    assert str(mdc.get_group("Group1")) == str(group_md1)


def test_get_group_w_features(mdc):
    assert str(mdc.get_group("Group2")) == str(group_md2)


def test_get_group_none(mdc):
    with pytest.raises(Exception):
        mdc.get_group("Group42")


def test_get_feature(mdc):
    assert str(mdc.get_feature("Group2", "Feature1")) == str(feature_md1)


def test_get_feature_none_group(mdc):
    with pytest.raises(Exception):
        mdc.get_feature("Group42", "Feature1")


def test_get_feature_none_feature(mdc):
    with pytest.raises(Exception):
        mdc.get_feature("Group2", "Feature8")
