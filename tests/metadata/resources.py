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
from pyspark.sql.types import ArrayType, StringType, StructField, StructType

from rialto.metadata import FeatureMetadata, GroupMetadata, Schedule, ValueType

group_schema = StructType(
    [
        StructField("group_name", StringType(), False),
        StructField("group_frequency", StringType(), False),
        StructField("group_description", StringType(), False),
        StructField("group_key", ArrayType(StringType(), True), False),
        StructField("group_fs_name", StringType(), False),
        StructField("group_owner", StringType(), False),
    ]
)

feature_schema = StructType(
    [
        StructField("feature_name", StringType(), True),
        StructField("feature_type", StringType(), True),
        StructField("feature_description", StringType(), True),
        StructField("group_name", StringType(), True),
    ]
)

group_base = [
    ("Group1", "weekly", "group1", ["key1"], "group_1", "owner_1"),
    ("Group2", "monthly", "group2", ["key2", "key3"], "group_2", "owner_2"),
]

feature_base = [
    ("Feature1", "nominal", "feature1", "Group2"),
    ("Feature2", "nominal", "feature2", "Group2"),
]

group_md1 = GroupMetadata(
    name="Group1",
    fs_name="group_1",
    frequency=Schedule.weekly,
    description="group1",
    key=["key1"],
    owner="owner_1",
)

group_md2 = GroupMetadata(
    name="Group2",
    fs_name="group_2",
    frequency=Schedule.monthly,
    description="group2",
    key=["key2", "key3"],
    owner="owner_2",
    features=["Feature1", "Feature2"],
)

feature_md1 = FeatureMetadata(name="Feature1", value_type=ValueType.nominal, description="feature1", group=group_md2)
