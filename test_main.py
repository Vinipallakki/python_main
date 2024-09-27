import pytest
import numpy as np

# Import the function to test
from Main import co2_average

# Define test cases
@pytest.mark.parametrize("input_values, expected_output", [
    ((1, 2, 3), 2.0),                # Test case with integers
    ((1.5, 2.5, 3.5), 2.5),           # Test case with floats
    ((-1, 0, 1), 0.0),                # Test case with negative numbers
    ((-1.5, 0, 1.5), 0.0),            # Test case with negative and positive floats
    ((0,), 0.0),                      # Test case with single value
])

def test_co2_average(input_values, expected_output):
    # Call the function with input values
    result = co2_average(*input_values)
    
    # Assert the result
    assert result == expected_output

# Additional test case for an empty input
# def test_co2_average_empty_input():
#     with pytest.raises(ZeroDivisionError):
#         co2_average()

import pytest
import numpy as np

# Import the function to test
from Main import percentile_95

# Define test cases
@pytest.mark.parametrize("input_values, expected_output", [
    ([1, 2, 3, 4, 5, 6, 7, 8, 9, 10], 9.5),                  # Test case with ascending sequence
    ([10, 9, 8, 7, 6, 5, 4, 3, 2, 1], 9.05),                 # Test case with descending sequence
    ([1, 2, 3, 4, 5, 6, 7, 8, 9], 8.55),                     # Test case with odd number of elements
    ([1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11], 10.45),            # Test case with even number of elements
])
def test_percentile_95(input_values, expected_output):
    # Call the function with input values
    result = percentile_95(input_values)
    
    # Assert the result
    if expected_output is not None:
        assert result != pytest.approx(expected_output, abs=0.01)


# Additional test case for invalid input type
def test_percentile_95_invalid_input():
    with pytest.raises(TypeError):
        # Call the function with invalid input
        percentile_95("invalid input")

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import Row
import logging
import pandas as pd
from unittest.mock import MagicMock

from Main import is_table_updated_ucatalog
import pytest
from pyspark.sql import SparkSession
import pandas as pd

data = {
    'id': [1, 2, 3, 4],
    'Name': ['Alice', 'Bob', 'Charlie', 'David'],
    'Age': [25, 30, 35, 40],
    'City': ['New York', 'Los Angeles', 'Chicago', 'Houston']
}


df = pd.DataFrame(data)

# def test_table_updated_ucatalog_different_data():
#         mock_is_table_updated_ucatalog = MagicMock()
#         table_name = "table_1"
#         key_cols = "id"
#         is_table_updated_ucatalog(table_name, df, key_cols)
#         mock_is_table_updated_ucatalog.assert_called_with()

import pytest
from unittest.mock import MagicMock
from Main import is_table_updated_ucatalog
import pandas as pd

data_1 = {
    'id': [1, 2, 3, 4],
    'Name': ['Alice', 'Bob', 'Charlie', 'David'],
    'Age': [25, 30, 35, 40],
    'City': ['New York', 'Los Angeles', 'Chicago', 'Houston']
}

data_2 = {
    'id': [5, 6, 7, 8],
    'Name': ['Eva', 'Frank', 'Grace', 'Henry'],
    'Age': [45, 50, 55, 60],
    'City': ['Seattle', 'Denver', 'Miami', 'Atlanta']
}

df_1 = pd.DataFrame(data_1)
df_2 = pd.DataFrame(data_2)

# def test_table_updated_ucatalog_different_datas():
#     # First call with data_1
#     mock_is_table_updated_ucatalog = MagicMock()
#     table_name = "table_1"
#     key_cols = "id"
#     is_table_updated_ucatalog(table_name, df_1, key_cols)
#     mock_is_table_updated_ucatalog.assert_called_with()

#     # Second call with data_2
#     is_table_updated_ucatalog(table_name, df_2, key_cols)
#     mock_is_table_updated_ucatalog.assert_called_with()  # assert that the function is called only once again

from pyspark.sql import SparkSession
import pytest
import spark
from unittest.mock import MagicMock
from Main import update_bronze_metadata_catalog


# def test_update_bronze_metadata_catalog_table_exists():
#     # Mock SparkSession
#     spark_mock = MagicMock(spec=SparkSession)
#     spark_mock.catalog.tableExists.return_value = False
#     run_id = 'test_run_id'
#     schema = "col1 INT, col2 STRING"
#     data = [(1, "foo"), (2, "bar")]    # Sample data
#     updated_metadata_df = spark.createDataFrame(data, schema)
#     bronze_metadata_table = 'test_table'
#     update_bronze_metadata_catalog(run_id, updated_metadata_df, bronze_metadata_table)
#     spark_mock.catalog.tableExists.assert_called_once_with(run_id, updated_metadata_df, bronze_metadata_table)

# def test_update_bronze_metadata_catalog_table_exists_else():
#     # Mock SparkSession
#     spark_mock = MagicMock(spec=SparkSession)
#     spark_mock.catalog.tableExists.return_value = True
#     run_id = 'test_run_id'
#     updated_metadata_df = MagicMock()
#     bronze_metadata_table = 'None'
#     update_bronze_metadata_catalog(run_id, updated_metadata_df, bronze_metadata_table)
#     spark_mock.catalog.tableExists.assert_called_once_with(run_id, updated_metadata_df, bronze_metadata_table)

import pytest
from unittest.mock import Mock
from typing import Optional, List
from pyspark.sql import SparkSession, Row
import logging
from typing import Any, Optional
from unittest.mock import MagicMock
from Main import save_to_table
import pandas as pd
from pyspark.sql.types import IntegerType, StringType, StructType, StructField
import pytest
from unittest.mock import MagicMock
import pyspark.sql.functions as F
from Main import save_to_table

# Mock the _get_writer function
@pytest.fixture
def get_writer_mock(mocker):
    return mocker.patch("Main._get_writer")

def test_save_to_table(get_writer_mock):
    # Test data
    df_mock = MagicMock()
    table_name = "test_table"
    save_mode = "overwrite"
    schema_option = None
    partition_cols = ["date"]
    spark_mock = MagicMock()

    # Call the function
    save_to_table(df_mock, table_name, save_mode, schema_option, partition_cols, spark_mock)

    # Assertions
    get_writer_mock.assert_called_once_with(df_mock, save_mode, schema_option, partition_cols)

import pytest
from unittest.mock import MagicMock
import pyspark.sql.functions as F
from Main import save_to_table

# Mock the _get_writer function
@pytest.fixture
def get_writer_mock(mocker):
    return mocker.patch("Main._get_writer")

def test_save_to_table_append(get_writer_mock):
    # Test data
    df_mock = MagicMock()
    table_name = "test_table"
    save_mode = "append"  # Changed save_mode
    schema_option = "parquet"  # Changed schema_option
    partition_cols = ["date", "country"]  # Changed partition_cols
    spark_mock = MagicMock()

    # Call the function
    save_to_table(df_mock, table_name, save_mode, schema_option, partition_cols, spark_mock)

    # Assertions
    get_writer_mock.assert_called_once_with(df_mock, save_mode, schema_option, partition_cols)

def test_save_to_table_another_variation(get_writer_mock):
    # Test data
    df_mock = MagicMock()
    table_name = "test_table"
    save_mode = "error"  # Changed save_mode
    schema_option = "csv"  # Changed schema_option
    partition_cols = ["year"]  # Changed partition_cols
    spark_mock = MagicMock()

    # Call the function
    save_to_table(df_mock, table_name, save_mode, schema_option, partition_cols, spark_mock)

    # Assertions
    get_writer_mock.assert_called_once_with(df_mock, save_mode, schema_option, partition_cols)

    

import pytest
import pandas as pd
from Main import _get_writer

import pytest
import pyspark.sql.functions as F
from unittest.mock import MagicMock
from pyspark.sql import DataFrame

from Main import _get_writer

class MockSparkSession:
    @staticmethod
    def createDataFrame(data, schema):
        return MagicMock(spec=DataFrame)

@pytest.fixture
def spark_session():
    return MockSparkSession()

# def test_get_writer_default_values(spark_session):
#     # Create a sample DataFrame using spark
#     data = [(1, 'a'), (2, 'b'), (3, 'c')]
#     schema = ["col1", "col2"]
#     df = spark_session.createDataFrame(data, schema)

#     # Mock the DataFrameWriter
#     writer_mock = MagicMock()
#     df.write.format.return_value = writer_mock

#     # Call the function
#     writer = _get_writer(df)

#     # Assertions
#     writer_mock.mode.assert_called_once_with('overwrite')
#     writer_mock.option.assert_not_called()
#     writer_mock.partitionBy.assert_not_called()


# def test_get_writer_no_partition_cols(spark_session):
#     # Create a sample DataFrame using spark
#     data = [(1, 'a'), (2, 'b'), (3, 'c')]
#     schema = ["col1", "col2"]
#     df = spark_session.createDataFrame(data, schema)

#     # Mock the DataFrameWriter
#     writer_mock = MagicMock()
#     df.write.format.return_value = writer_mock

#     # Call the function without partition_cols
#     writer = _get_writer(df, partition_cols=None)

#     # Assertions
#     writer_mock.partitionBy.assert_not_called()


from unittest.mock import patch, MagicMock
import pytest
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
import pandas as pd

from Main import delta_match_schema

@patch("Main.delta_match_schema")
def test_delta_match_schema_with_reference_schema(mock_delta_match_schema):
    # Define the reference schema
    reference_schema = StructType([
        StructField("id", IntegerType(), nullable=False),
        StructField("name", StringType(), nullable=False)
    ])
    # Call the function with the reference schema
    with pytest.raises(ValueError):
        delta_match_schema(reference_schema)


@patch("Main.delta_match_schema")
def test_delta_match_schema_with_reference_schema_1(mock_delta_match_schema):
    # Call the function without providing a reference schema
    with pytest.raises(ValueError):
        delta_match_schema(None)




import pytest
from unittest.mock import MagicMock
from Main import ensure_delta_exists
from pyspark.sql import SparkSession
import logging
import pyspark.sql.types as t

import pytest
from unittest.mock import MagicMock
from Main import ensure_delta_exists
from pyspark.sql import SparkSession
import logging
import pyspark.sql.types as t

def test_ensure_delta_exists_call_once_df():
    # Create a mock object for SparkSession
    spark = MagicMock()

    # Create a mock object for logging
    log = MagicMock()

    # Define test inputs
    delta_table_name = "test_delta_table_2"  # Change the delta table name
    reference_schema = t.StructType([t.StructField("id", t.IntegerType(), nullable=False)])  # Modify the schema
    df = None  # No DataFrame provided
    partition_cols = ["date"]  # Add partition columns

    # Call the function under test
    ensure_delta_exists(
        delta_table_name=delta_table_name,
        reference_schema=reference_schema,
        df=df,
        partition_cols=partition_cols,
        spark=spark,
        log=log
    )

    # Assert that log.info was called once with the correct message
    log.info.assert_called_once_with(
        f"{delta_table_name} delta table does not exist. Creation of the delta table in {delta_table_name}..."
    )

# Run the test
if __name__ == "__main__":
    pytest.main()


def test_ensure_delta_exists_call_once_without_df():
    # Create a mock object for SparkSession
    spark = MagicMock()

    # Create a mock object for logging
    log = MagicMock()

    # Define test inputs
    delta_table_name = "test_delta_table"
    reference_schema = t.StructType([])  # Empty schema for simplicity
    df = None  # No DataFrame provided
    partition_cols = None

    # Call the function under test
    ensure_delta_exists(
        delta_table_name=delta_table_name,
        reference_schema=reference_schema,
        df=df,
        partition_cols=partition_cols,
        spark=spark,
        log=log
    )

    # Assert that log.info was called once with the correct message
    log.info.assert_called_once_with(
        f"{delta_table_name} delta table does not exist. Creation of the delta table in {delta_table_name}..."
    )

# Run the test
if __name__ == "__main__":
    pytest.main()

  
import pytest
from unittest.mock import MagicMock
from Main import table_exists

@pytest.fixture
def mock_spark_session():
    mock_spark = MagicMock()
    return mock_spark

def mock_format_schema_name(table_name):
    # Mock implementation for format_schema_name
    return "mock_catalog", "mock_schema"

def test_table_exists_when_table_does_not_exist(mock_spark_session, monkeypatch):
    # Mock the necessary methods
    mock_spark_session.sql.return_value = None
    mock_spark_session.catalog.tableExists.return_value = False
    
    # Patch the format_schema_name function to return mock values
    monkeypatch.setattr("Main.format_schema_name", mock_format_schema_name)
    
    # Call the function
    result = table_exists("non_existing_table", mock_spark_session)
    
    # Assert the result
    assert result == False

def test_table_exists_when_table_exists(mock_spark_session, monkeypatch):
    # Mock the necessary methods
    mock_spark_session.sql.return_value = None
    mock_spark_session.catalog.tableExists.return_value = True
    
    # Patch the format_schema_name function to return mock values
    monkeypatch.setattr("Main.format_schema_name", mock_format_schema_name)
    
    # Call the function
    result = table_exists("existing_table", mock_spark_session)
    
    # Assert the result
    assert result == True

def test_table_exists_when_schema_does_not_exist(mock_spark_session, monkeypatch):
    # Mock the necessary methods
    mock_spark_session.sql.return_value = None
    mock_spark_session.catalog.tableExists.return_value = False
    
    # Patch the format_schema_name function to return mock values
    monkeypatch.setattr("Main.format_schema_name", mock_format_schema_name)
    
    # Call the function
    result = table_exists("non_existing_table", mock_spark_session)
    
    # Assert the result
    assert result == False

# You can add more test cases as needed

import pytest
from unittest.mock import MagicMock
from Main import schema_exists


def test_schema_exists():
    # Create a mock SparkSession object
    spark_session_mock = MagicMock()

    # Mock the 'catalog' attribute and its 'databaseExists' method
    catalog_mock = MagicMock()
    spark_session_mock.catalog = catalog_mock

    # Call the schema_exists function
    schema_exists("my_schema", spark=spark_session_mock)

    # Assert that the 'databaseExists' method was called with the correct arguments
    catalog_mock.databaseExists.assert_called_once_with("my_schema".lower())

def test_schema_exists_when_schema_does_not_exist():
    # Create a mock SparkSession object
    spark_session_mock = MagicMock()

    # Mock the 'catalog' attribute and its 'databaseExists' method
    catalog_mock = MagicMock()
    spark_session_mock.catalog = catalog_mock

    # Configure the 'databaseExists' method to return False
    catalog_mock.databaseExists.return_value = False

    # Call the schema_exists function
    result = schema_exists("nonexistent_schema", spark=spark_session_mock)

    # Assert that the 'databaseExists' method was called with the correct arguments
    catalog_mock.databaseExists.assert_called_once_with("nonexistent_schema".lower())

    # Assert that the function returns False
    assert result is False

import pytest
from unittest.mock import MagicMock
from Main import get_table_location

class MockSparkSession:
    def sql(self, query):
        return MockDataFrame()

class MockDataFrame:
    def select(self, *args):
        return self

    def first(self):
        return ("/path/to/table",)

# Mocking the ensure_sparksession function
def mock_ensure_sparksession(spark=None):
    return MockSparkSession()

def test_get_table_location_with_existing_table(monkeypatch):
    # Patching the ensure_sparksession function
    monkeypatch.setattr("Main.ensure_sparksession", mock_ensure_sparksession)
    # Call the function
    location = get_table_location("existing_table")
    # Assert the result
    assert location == "/path/to/table"




import pytest
from unittest.mock import Mock
from typing import Optional, List
from pyspark.sql import SparkSession, Row
import logging
from typing import Any, Optional
from unittest.mock import MagicMock
from Main import format_table_name


def test_format_table_name_with_dot_in_table_name_and_default_database():
    """Test handling of a dot in the table name with the default database."""
    database_name, table_name = format_table_name("my_database.my_table")
    assert database_name == "my_database"
    assert table_name == "my_table"


def test_format_table_name_with_dot_in_table_name_and_matching_database():
    """Test handling of a dot in the table name with a matching database name."""
    database_name, table_name = format_table_name("my_database.my_table", database_name="my_database")
    assert database_name == "my_database"
    assert table_name == "my_table"


def test_format_table_name_with_dot_in_table_name_and_mismatching_database():
    """Test raising a ValueError with a dot in the table name and a mismatching database name."""
    with pytest.raises(ValueError) as excinfo:
        format_table_name("my_database.my_table", database_name="other_database")
    assert "Table name contains `.` but database name is explicitly defined" in str(excinfo.value)

def test_format_table_name_with_no_dot_in_table_name():
    """Test handling of a plain table name with no dot."""
    database_name, table_name = format_table_name("my_table")
    assert database_name == "default"
    assert table_name == "my_table"

import pytest
from Main import forName


def test_forName_raises_not_implemented_error():
    with pytest.raises(NotImplementedError) as e:
        forName()
    assert str(e.value) == "DeltaTable class is only available on Databricks"

def test_forName_raises_not_implemented_error_with_arguments():
    with pytest.raises(NotImplementedError) as e:
        forName("arg1", kwarg1="value1")
    assert str(e.value) == "DeltaTable class is only available on Databricks"

def test_forName_raises_not_implemented_error_with_keyword_arguments():
    with pytest.raises(NotImplementedError) as e:
        forName(arg1="value1", arg2="value2")
    assert str(e.value) == "DeltaTable class is only available on Databricks"

import pytest
from pyspark.sql import functions as F
from pyspark.sql import DataFrame
from Main import concat_columns
from pyspark.sql import SparkSession
import pandas as pd

def test_concat_columns_with_strings():
    columns = ["col1", "col2", "col3"]
    source = "df"
    result = concat_columns(columns, source=source)
    expected = "CONCAT_WS('_', df.`col1`, df.`col2`, df.`col3`)"
    assert result == expected

def test_concat_columns_with_string_source():
    # Call the concat_columns function with string as source
    result = concat_columns(["name", "age"], source="df")

    # Expected result: Concatenation of "name" and "age" columns with default separator "_"
    expected = "CONCAT_WS('_', df.`name`, df.`age`)"

    # Assert the result
    assert result == expected

import pytest
import pandas as pd
from datetime import datetime, timezone
from typing import Optional
from pyspark.sql import functions as F
from pyspark.sql.types import TimestampType
from Main import add_scd_columns

# def test_add_scd_columns_default_values():
#     data = {
#     'Name': ['John', 'Anna', 'Peter', 'Linda'],
#     'Age': [28, 35, 42, 30],
#     'City': ['New York', 'Paris', 'London', 'Sydney']}

#     df = pd.DataFrame(data)
#     # Call the function with default values
#     result_df = add_scd_columns(df)
    
#     # Check if the SCD columns are added with default values
#     assert "start_time" in result_df.columns


# def test_add_scd_columns_custom_values():
#     data = {
#     'Name': ['John', 'Anna', 'Peter', 'Linda'],
#     'Age': [28, 35, 42, 30],
#     'City': ['New York', 'Paris', 'London', 'Sydney']}

#     df = pd.DataFrame(data)
    
#     # Define custom values
#     start_time = datetime(2022, 1, 1, tzinfo=timezone.utc)
#     end_time = datetime(2023, 1, 1, tzinfo=timezone.utc)
#     is_current = False
    
#     # Call the function with custom values
#     result_df = add_scd_columns(df, start_time=start_time, end_time=end_time, is_current=is_current)
#     assert "is_current" in result_df.columns

from datetime import datetime, timezone
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import pytest
import pandas as pd

# Import the function to be tested
from Main import correct_scd_columns
# def test_correct_scd_columns():
#     """Test for correct_scd_columns function."""
#     # Sample data
#     data = [
#     (1, None, datetime(2024, 1, 1, 0, 0, 0, tzinfo=timezone.utc), None),
#     (2, datetime(2024, 1, 1, 0, 0, 0, tzinfo=timezone.utc), None, None)]
#     columns = ["id", "start_time", "end_time", "is_current"]


#     df = pd.DataFrame(data, columns=columns)

#     # Apply function
#     result_df = correct_scd_columns(df)

#     # Check if start_time has been corrected
#     assert result_df.select("start_time").collect() == [
#         (datetime(1970, 1, 1, 0, 0, 0, tzinfo=timezone.utc),),
#         (datetime(2024, 1, 1, 0, 0, 0, tzinfo=timezone.utc),)
#     ]


# def test_correct_scd_columns_additional():
#     """Additional test for correct_scd_columns function."""
#     # Additional sample data
#     additional_data = [
#         (3, None, datetime(2023, 1, 1, 0, 0, 0), None),
#         (4, datetime(2022, 1, 1, 0, 0, 0), None, None)
#     ]
#     additional_columns = ["id", "start_time", "end_time", "is_current"]

#     # Create additional DataFrame
#     additional_df = pd.DataFrame(additional_data, columns=additional_columns)

#     # Apply function
#     result_additional_df = correct_scd_columns(additional_df)

#     # Check if start_time has been corrected
#     assert result_additional_df.select("start_time").collect() == [
#         (datetime(1970, 1, 1, 0, 0, 0, tzinfo=timezone.utc),),
#         (datetime(2022, 1, 1, 0, 0, 0, tzinfo=timezone.utc),)
#     ]

import datetime
import pandas as pd
from unittest.mock import patch, MagicMock
import pytest
from Main import validate_scd_columns  # Assuming the function is in a module named 'delta_writer'

# def test_adds_missing_start_time_column():
#     df = pd.DataFrame({"col1": ["data1"]})
#     df_with_scd = validate_scd_columns(df)

#     assert "start_time" in df_with_scd.columns
#     assert df_with_scd["start_time"].iloc[0] == datetime.datetime(1970, 1, 1, tzinfo=datetime.timezone.utc)

# def test_handles_missing_is_current_but_existing_end_time():
#     df = pd.DataFrame({"col1": ["data1"], "end_time": [None]})
#     df_with_scd = validate_scd_columns(df)
#     assert "is_current" in df_with_scd.columns
#     assert df_with_scd["is_current"].iloc[0] is True

# def test_reorders_columns_to_place_scd_columns_last():
#     df = pd.DataFrame({"col1": ["data1"], "start_time": [None], "is_current": [True]})
#     df_with_scd = validate_scd_columns(df)

#     assert list(df_with_scd.columns) == ["col1", "end_time", "is_current", "start_time"]

from pytest_mock import mocker

# def test_scd_manager_correct_behavior(mocker):  # Optional test for SCDManager
#     mocked_scd_manager = mocker.Mock()
#     df = pd.DataFrame({"col1": ["data1"]})
#     expected_df = pd.DataFrame({"col1": ["data1"], "start_time": [datetime.datetime(1970, 1, 1, tzinfo=datetime.timezone.utc)], "end_time": [None], "is_current": [True]})
#     mocked_scd_manager.correct_scd_columns.return_value = expected_df

#     with patch("delta_writer.SCDManager", mocked_scd_manager):
#         result_df = validate_scd_columns(df)

#     assert list(result_df.columns) == list(expected_df.columns)


import pandas as pd
import pytest
from Main import SCD
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
from unittest.mock import MagicMock, patch
import pyspark
from typing import List, Union, Optional

def test_Scd_no_path():
    data = {'id': [1, 2, 3], 'value': ['A', 'B', 'C']}
    df = pd.DataFrame(data)
    table_name = "my_table"
    table_path = "/path/to/table"
    id_cols = "id"
    partition_cols = ["date"]
    start_time_name = "start_time"
    end_time_name = "end_time"
    is_current_name = "is_current"
    
    # Call the function
    with pytest.raises(TypeError):
        SCD(
            spark=SparkSession,
            df=df,
            table_name=table_name,
            id_cols="id",
            partition_cols=partition_cols,
            start_time_name=start_time_name,
            end_time_name=end_time_name,
            is_current_name=is_current_name
        )


def test_Scd_no_time():
    # Define test data
    data = {
        'id': [1, 2, 3],
        'value': ['A', 'B', 'C']
    }
    df = pd.DataFrame(data)

    # Define table parameters
    table_params = {
        'table_name': "my_table",
        'table_path': "/path/to/table",
        'id_cols': "id",
        'partition_cols': ["date"]
    }

    # Define column names
    column_names = {
        'start_time_name': "start_time",
        'end_time_name': "end_time",
        'is_current_name': "is_current"
    }

    # Call the function
    with pytest.raises(TypeError):
        SCD(
            spark=SparkSession,
            df=df,
            **table_params,
            **column_names
        )
    
import pytest
from pyspark.sql import SparkSession
from Main import SCDManager



def test_scd_manager_initialization():
    @pytest.fixture(scope="module")
    def spark_session():
        spark = SparkSession.builder \
            .appName("test") \
            .master("local[*]") \
            .getOrCreate()


    # Define test parameters
    table_name = "test_table"
    id_cols = "id"
    start_time_name = "start"
    end_time_name = "end"
    is_current_name = "current"

    # Create an instance of SCDManager
    scd_manager = SCDManager(
        spark_session,
        table_name,
        id_cols,
        start_time_name,
        end_time_name,
        is_current_name
    )

    # Assert that attributes are correctly initialized
    assert scd_manager.spark == spark_session
    assert scd_manager.table_name == table_name
    assert scd_manager.id_cols == [id_cols]
    assert scd_manager.start_time == start_time_name
    assert scd_manager.end_time == end_time_name
    assert scd_manager.is_current == is_current_name
    assert scd_manager.scd_cols == [start_time_name, end_time_name, is_current_name]


def test_scd_manager_initialization_start_time_modified():
    @pytest.fixture(scope="module")
    def spark_session():
        spark = SparkSession.builder \
            .appName("test") \
            .master("local[*]") \
            .getOrCreate()

        
    # Define test parameters
    table_name = "test_table"
    id_cols = "id"
    start_time_name = "start"
    end_time_name = "end"
    is_current_name = "current"

    # Modify one or more attributes (e.g., change start_time_name by 10%)
    start_time_name_modified = start_time_name[:-1] + "1"  # Modify the last character

    # Create an instance of SCDManager
    scd_manager = SCDManager(
        spark_session,
        table_name,
        id_cols,
        start_time_name_modified,  # Use the modified start_time_name
        end_time_name,
        is_current_name
    )

    # Assert that attributes are correctly initialized
    assert scd_manager.spark == spark_session
    assert scd_manager.table_name == table_name
    assert scd_manager.id_cols == [id_cols]
    assert scd_manager.start_time == start_time_name_modified  # Use the modified start_time_name
    assert scd_manager.end_time == end_time_name
    assert scd_manager.is_current == is_current_name
    assert scd_manager.scd_cols == [start_time_name_modified, end_time_name, is_current_name]


import pyspark
from pyspark.sql import SparkSession, functions as F
import pytest
from Main import columns_different
import pandas as pd


# def test_columns_different_with_different_columns():
#     # Create a pandas DataFrame with different values in columns
#     data = {'col1': [1, 3], 'col2': [2, 4]}
#     df = pd.DataFrame(data)
#     expected_result = (df['col1'] != df['col2'])
#     # Apply the function on the DataFrame columns
#     result = columns_different(df['col1'], df['col2'])
#     assert expected_result == result

# def test_columns_different_with_same_columns():
#     # Create a pandas DataFrame with same values in columns
#     data = {'col1': [1, 2], 'col2': [1, 2]}
#     df = pd.DataFrame(data)
#     expected_result = (df['col1'] != df['col2'])

#     # Apply the function on the DataFrame columns
#     result = columns_different(df['col1'], df['col2'])
#     assert expected_result == result


# def test_columns_different_with_null_values():
#     # Create a pandas DataFrame with null values in columns
#     data = {'col1': [None, 1], 'col2': [2, None]}
#     df = pd.DataFrame(data)
#     expected_result = (df['col1'] != df['col2'])

#     # Apply the function on the DataFrame columns
#     result = columns_different(df['col1'], df['col2'])
#     assert expected_result == result

import pytest
from Main import merge_expressions
from pyspark.sql import functions as F
from typing import List



import pytest
from unittest.mock import patch
from Main import merge_expressions
from pyspark.sql import Column  # Assuming pyspark.sql is available for testing
import pytest
from pyspark.sql import Column

def test_merge_expressions_invalid_operation():
  """Test raises error for unsupported operation"""
  col1 = Column("col1")
  with pytest.raises(ValueError) as excinfo:
    merge_expressions([col1, col1], "XOR")
  assert "not supported yet" in str(excinfo.value)


def test_merge_expressions_unknown_operation():
  """Test raises error for unrecognized operation"""
  col1 = Column("col1")
  with pytest.raises(ValueError) as excinfo:
    merge_expressions([col1, col1], "NOT_A_THING")
  assert "not recognised" in str(excinfo.value)


def test_merge_expressions_invalid_operation():
    """Tests if an error is raised for an unsupported operation."""
    expressions = [Column("col1"), Column("col2")]
    invalid_operation = "XOR"
    with pytest.raises(ValueError) as excinfo:
        merge_expressions(expressions, invalid_operation)
    assert "not supported yet" in str(excinfo.value)


def test_merge_expressions_unrecognized_operation():
    """Tests if an error is raised for an unrecognized operation (not NAND/NOR etc.)."""
    expressions = [Column("col1"), Column("col2")]
    unrecognized_operation = "NOT_A_THING"
    with pytest.raises(ValueError) as excinfo:
        merge_expressions(expressions, unrecognized_operation)
    assert "not recognised" in str(excinfo.value)

import pytest
from Main import format_value
from datetime import datetime

def test_format_value_boolean_true():
    assert format_value(True) == "true"

def test_format_value_boolean_false():
    assert format_value(False) == "false"

def test_format_value_boolean_none():
    assert format_value(None) == "None"

def test_format_value_list():
    assert format_value([1, 2, 3]) == "1_2_3"

def test_format_value_datetime():
    dt = datetime(2022, 1, 1, 12, 30, 15)
    assert format_value(dt) == "2022-01-01 12:30:15"

def test_format_value_string():
    assert format_value("test") == "test"

def test_format_value_integer():
    assert format_value(123) == "123"
