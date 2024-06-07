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

from pyspark.sql.types import FloatType, IntegerType, StringType

feature_group_a_data = [("K1", 1, "A"), ("K2", 2, "A"), ("K3", 3, None)]
feature_group_a_columns = [("KEY", StringType()), ("A1", IntegerType()), ("A2", StringType())]

base_frame_data = [("K1",), ("K2",), ("K3",)]
base_frame_columns = [("KEY1", StringType())]

mapping1_data = [("M1", "K1", "N11"), ("M2", "K3", "N23")]
mapping1_columns = [("KEY2", StringType()), ("KEY1", StringType()), ("KEY3", StringType())]

mapping2_data = [("K1", "M1"), ("K2", "M1"), ("K3", "M2"), ("K4", "M2")]
mapping2_columns = [("KEY1", StringType()), ("KEY2", StringType())]

mapping3_data = [("N11", "H5"), ("N23", "H6")]
mapping3_columns = [("KEY3", StringType()), ("KEY4", StringType())]

expected_mapping_data = [("K1", "M1", "N11", "H5"), ("K3", "M2", "N23", "H6")]
expected_mapping_columns = [
    ("KEY1", StringType()),
    ("KEY2", StringType()),
    ("KEY3", StringType()),
    ("KEY4", StringType()),
]

feature_group_b_data = [("K1", "A", 5, None), ("K3", "B", 7, 0.36)]
feature_group_b_columns = [("KEY1", StringType()), ("F1", StringType()), ("F2", IntegerType()), ("F3", FloatType())]

expected_features_b_data = [("K1", "A", None), ("K2", None, None), ("K3", "B", 0.36)]
expected_features_b_columns = [("KEY1", StringType()), ("B_F1", StringType()), ("B_F3", FloatType())]
