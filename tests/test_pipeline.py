import pandas as pd
import os

import sys
# sys.path.append("/opt/airflow/plugins")
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from clean_date import cast_data_types, to_snake_case, normalize_datetime
from handle_missing_value import handle_missing_value

def test_normalize_datetime():
    # Mock data column โดยปกติจะเป็น pd.Series
    dates = pd.Series(["2020-01-22 17:00", "01/23/2020 17:00", "2020/01/24 17:00", None])
    expected = ["2020-01-22 17:00", "2020-01-23 17:00", "2020-01-24 17:00", None]
    
    result = normalize_datetime(dates)
    assert list(result == expected)

def test_to_snake_case():
    assert to_snake_case("Column Name") == "column_name"
    assert to_snake_case("Column-Name") == "column_name"
    assert to_snake_case("Column@Name!") == "columnname"

def test_cast_data_types():
    data = {
        "sno": ["1", "2"],
        "province_state": ["State1", "State2"],
        "country_region": ["Country1", "Country2"],
        "observationdate": ["01/22/2020", "01/23/2020"],
        "last_update": ["2020-01-22 17:00", "2020-01-23 17:00"],
        "confirmed": ["10", "20"],
        "deaths": ["1", "2"],
        "recovered": ["0", "1"]
    }
    df = pd.DataFrame(data)
    df = cast_data_types(df)
    
    assert df["sno"].dtype == 'int64'
    assert df["province_state"].dtype == 'string'
    assert df["country_region"].dtype == 'string'
    assert pd.api.types.is_datetime64_any_dtype(df["observationdate"])
    assert pd.api.types.is_datetime64_any_dtype(df["last_update"])
    assert df["confirmed"].dtype == 'int64'
    assert df["deaths"].dtype == 'int64'
    assert df["recovered"].dtype == 'int64'


def test_handle_missing_value():
   
    # Create a sample DataFrame with missing values
    data = {
        "sno": [1, 2, 3],
        "province_state": ["State1", None, "State3"],
        "country_region": ["Country1", "Country2", None],
        "observationdate": ["2020-01-22", None, "2020-01-24"],
        "last_update": ["2020-01-22 17:00", "2020-01-23 17:00", None],
        "confirmed": [10, None, 30],
        "deaths": [1, None, 3],
        "recovered": [0, None, 5]
    }
    df = pd.DataFrame(data)
    
    # Save to a temporary CSV file
    input_path = "temp_input.csv"
    output_path = "temp_output.csv"
    df.to_csv(input_path, index=False)
    
    # Run the handle_missing_value function
    result_df = handle_missing_value(input_path, output_path)
    
    # Check that missing values are handled correctly
    assert result_df["province_state"].isnull().sum() == 0
    assert result_df["country_region"].isnull().sum() == 0
    assert result_df["observationdate"].isnull().sum() == 0
    assert result_df["last_update"].isnull().sum() == 0
    assert result_df["confirmed"].isnull().sum() == 0
    assert result_df["deaths"].isnull().sum() == 0
    assert result_df["recovered"].isnull().sum() == 0
    
    # Clean up temporary files
    os.remove(input_path)
    os.remove(output_path)


if __name__ == "__main__":
    test_normalize_datetime()
    test_to_snake_case()
    test_cast_data_types()
    test_handle_missing_value()
    print("All tests passed.")
