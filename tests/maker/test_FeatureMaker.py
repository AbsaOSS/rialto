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
from datetime import date

import pandas as pd
import pytest

from rialto.maker.feature_maker import FeatureMaker
from rialto.metadata import ValueType
from tests.maker.test_features import (
    aggregated_num_sum_outbound,
    aggregated_num_sum_txn,
    dependent_features_fail,
    dependent_features_fail2,
    dependent_features_ok,
    sequential_avg_outbound,
    sequential_avg_txn,
    sequential_for_testing,
    sequential_outbound,
    sequential_outbound_with_param,
)


@pytest.fixture
def input_df(spark):
    df = pd.DataFrame(
        [
            [42, "A", "C_1"],
            [-35, "A", "C_1"],
            [-12, "B", "C_1"],
            [-65, "B", "C_1"],
            [12, "A", "C_2"],
            [16, "A", "C_2"],
            [-10, "A", "C_2"],
        ],
        columns=["AMT", "TYPE", "CUSTOMER_KEY"],
    )
    return spark.createDataFrame(df)


def test_sequential_column_exists(input_df):
    df, _ = FeatureMaker.make(input_df, "CUSTOMER_KEY", date.today(), sequential_outbound, keep_preexisting=True)
    assert "TRANSACTIONS_OUTBOUND_VALUE" in df.columns


def test_sequential_multi_key(input_df):
    df, _ = FeatureMaker.make(
        input_df, ["CUSTOMER_KEY", "TYPE"], date.today(), sequential_outbound, keep_preexisting=True
    )
    assert "TRANSACTIONS_OUTBOUND_VALUE" in df.columns


def test_sequential_keeps(input_df):
    df, _ = FeatureMaker.make(input_df, "CUSTOMER_KEY", date.today(), sequential_outbound, keep_preexisting=True)
    assert "AMT" in df.columns


def test_sequential_drops(input_df):
    df, _ = FeatureMaker.make(input_df, "CUSTOMER_KEY", date.today(), sequential_outbound, keep_preexisting=False)
    assert "AMT" not in df.columns


def test_sequential_key_not_dropped(input_df):
    df, _ = FeatureMaker.make(input_df, "CUSTOMER_KEY", date.today(), sequential_outbound, keep_preexisting=False)
    assert "CUSTOMER_KEY" in df.columns


def test_sequential_with_params_column_exists(input_df):
    df, _ = FeatureMaker.make(
        input_df, "CUSTOMER_KEY", date.today(), sequential_outbound_with_param, keep_preexisting=False
    )
    assert "TRANSACTIONS_OUTBOUND_VALUE_V_TYPE_A" in df.columns


def test_aggregated_column_exists(input_df):
    df, _ = FeatureMaker.make_aggregated(input_df, "CUSTOMER_KEY", date.today(), aggregated_num_sum_txn)
    assert "TRANSACTIONS_NUM_TRANSACTIONS" in df.columns


def test_aggregated_key_exists(input_df):
    df, _ = FeatureMaker.make_aggregated(input_df, "CUSTOMER_KEY", date.today(), aggregated_num_sum_txn)
    assert "CUSTOMER_KEY" in df.columns


def test_aggregated_multi_key_exists(input_df):
    df, _ = FeatureMaker.make_aggregated(input_df, ["CUSTOMER_KEY", "TYPE"], date.today(), aggregated_num_sum_txn)
    assert "CUSTOMER_KEY" in df.columns and "TYPE" in df.columns


def test_maker_metadata(input_df):
    df, metadata = FeatureMaker.make_aggregated(input_df, "CUSTOMER_KEY", date.today(), aggregated_num_sum_txn)
    assert metadata[0].value_type == ValueType.numerical


def test_double_chained_makers_column_exists(input_df):
    df, _ = FeatureMaker.make_aggregated(input_df, "CUSTOMER_KEY", date.today(), aggregated_num_sum_txn)
    df, _ = FeatureMaker.make(df, "CUSTOMER_KEY", date.today(), sequential_avg_txn)
    assert "TRANSACTIONS_AVG_TRANSACTION" in df.columns


def test_tripple_chained_makers_column_exists(input_df):
    # create outbound column
    df, _ = FeatureMaker.make(input_df, "CUSTOMER_KEY", date.today(), sequential_outbound)
    # agg outbound sum and num
    df, _ = FeatureMaker.make_aggregated(df, "CUSTOMER_KEY", date.today(), aggregated_num_sum_outbound)
    # create outbound avg
    df, _ = FeatureMaker.make(df, "CUSTOMER_KEY", date.today(), sequential_avg_outbound)
    assert "TRANSACTIONS_AVG_OUTBOUND" in df.columns


def test_tripple_chained_makers_key_exists(input_df):
    # create outbound column
    df, _ = FeatureMaker.make(input_df, "CUSTOMER_KEY", date.today(), sequential_outbound)
    # agg outbound sum and num
    df, _ = FeatureMaker.make_aggregated(df, "CUSTOMER_KEY", date.today(), aggregated_num_sum_outbound)
    # create outbound avg
    df, _ = FeatureMaker.make(df, "CUSTOMER_KEY", date.today(), sequential_avg_outbound)
    assert "CUSTOMER_KEY" in df.columns


def test_dependency_resolution(input_df):
    ordered = FeatureMaker._order_by_dependencies(FeatureMaker._load_features(dependent_features_ok))
    ordered = [f[0].name for f in ordered]
    assert ordered.index("f4_raw") == 0
    assert ordered.index("f3_depends_f2") < ordered.index("f1_depends_f3_f5")
    assert ordered.index("f5_depends_f4") < ordered.index("f1_depends_f3_f5")
    assert ordered.index("f4_raw") < ordered.index("f2_depends_f4")
    assert ordered.index("f2_depends_f4") < ordered.index("f3_depends_f2")
    assert ordered.index("f4_raw") < ordered.index("f5_depends_f4")


def test_dependency_resolution_cycle(input_df):
    with pytest.raises(Exception, match="Feature dependencies can't be resolved!"):
        FeatureMaker._order_by_dependencies(FeatureMaker._load_features(dependent_features_fail))


def test_dependency_resolution_self_reference(input_df):
    with pytest.raises(Exception, match="Feature dependencies can't be resolved!"):
        FeatureMaker._order_by_dependencies(FeatureMaker._load_features(dependent_features_fail2))


def test_find_single_feature():
    features = FeatureMaker._register_module(sequential_for_testing)
    feature = FeatureMaker._find_feature("FOR_TESTING_PARAM_B", features)
    assert feature.get_feature_name() == "FOR_TESTING_PARAM_B"


def test_make_single_feature_column_exists(input_df):
    out = FeatureMaker.make_single_feature(input_df, "FOR_TESTING_PARAM_B", sequential_for_testing)
    assert "FOR_TESTING_PARAM_B" in out.columns


def test_make_single_feature_column_single(input_df):
    out = FeatureMaker.make_single_feature(input_df, "FOR_TESTING_PARAM_B", sequential_for_testing)
    assert len(out.columns) == 1


def test_make_single_agg_feature_column_exists(input_df):
    out = FeatureMaker.make_single_agg_feature(
        input_df, "TRANSACTIONS_SUM_TRANSACTIONS", "CUSTOMER_KEY", aggregated_num_sum_txn
    )
    assert "TRANSACTIONS_SUM_TRANSACTIONS" in out.columns


def test_make_single_agg_feature_column_single(input_df):
    out = FeatureMaker.make_single_agg_feature(
        input_df, "TRANSACTIONS_SUM_TRANSACTIONS", "CUSTOMER_KEY", aggregated_num_sum_txn
    )
    assert len(out.columns) == 2


def test_make_single_agg_feature_multikey(input_df):
    out = FeatureMaker.make_single_agg_feature(
        input_df, "TRANSACTIONS_SUM_TRANSACTIONS", ["CUSTOMER_KEY", "TYPE"], aggregated_num_sum_txn
    )
    assert len(out.columns) == 3
