#  Copyright 2022-2026 ABSA Group Limited
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

import pyspark.sql.functions as F
import pytest
from numpy import dtype
from pyspark.sql.types import DoubleType, LongType, StringType

from rialto.common.utils import cast_decimals_to_floats, normalize_types


@pytest.fixture
def sample_df(spark):
    df = spark.createDataFrame(
        [(1, 2.33, "str", 4.55, 5.66), (1, 2.33, "str", 4.55, 5.66), (1, 2.33, "str", 4.55, 5.66)],
        schema="a long, b float, c string, d float, e float",
    )

    return df.select("a", "b", "c", F.col("d").cast("decimal"), F.col("e").cast("decimal(18,5)"))


@pytest.fixture
def sample_df2(spark):
    df = spark.createDataFrame(
        [(1, 2.33, "str", 4.55, 5.66, 4), (1, 2.33, "str", 4.55, 5.66, 5), (1, 2.33, "str", 4.55, 5.66, 6)],
        schema="a long, b float, c string, d float, e float, f int",
    )

    return df.select("a", "b", "c", F.col("d").cast("decimal"), F.col("e").cast("double"), "f")


def test_cast_decimals_to_floats(sample_df):
    df_fixed = cast_decimals_to_floats(sample_df)

    assert df_fixed.dtypes[3] == ("d", "float")
    assert df_fixed.dtypes[4] == ("e", "float")


def test_cast_decimals_to_floats_topandas_works(sample_df):
    df_fixed = cast_decimals_to_floats(sample_df)
    df_pd = df_fixed.toPandas()

    assert df_pd.dtypes.iloc[3] == dtype("float32")
    assert df_pd.dtypes.iloc[4] == dtype("float32")


def test_normalize_types(sample_df2):
    df_fixed = normalize_types(sample_df2)

    assert isinstance(df_fixed.schema["a"].dataType, LongType)
    assert isinstance(df_fixed.schema["b"].dataType, DoubleType)
    assert isinstance(df_fixed.schema["c"].dataType, StringType)
    assert isinstance(df_fixed.schema["d"].dataType, DoubleType)
    assert isinstance(df_fixed.schema["e"].dataType, DoubleType)
    assert isinstance(df_fixed.schema["f"].dataType, LongType)
