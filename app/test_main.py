from .main import filter_spark_data_frame
import pandas as pd


def test_filter_spark_data_frame(sql_context):
    input = sql_context.createDataFrame(
        [('charly', 16),
         ('fabien', 15),
         ('sam', 21),
         ('sam', 25),
         ('nick', 19),
         ('nick', 40)],
        ['name', 'age'],
    )
    expected_output = sql_context.createDataFrame(
        [('sam', 25),
         ('sam', 21),
         ('nick', 40)],
        ['name', 'age'],
    )
    real_output = filter_spark_data_frame(input)
    real_output = get_sorted_data_frame(
        real_output.toPandas(),
        ['age', 'name'],
    )
    expected_output = get_sorted_data_frame(
        expected_output.toPandas(),
        ['age', 'name'],
    )
    pd.testing.assert_frame_equal(expected_output, real_output, check_like=True)


def get_sorted_data_frame(data_frame, columns_list):
    return data_frame.sort_values(columns_list).reset_index(drop=True)
