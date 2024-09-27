import logging
from typing import List, Union, Optional, Any
import pyspark.sql.functions as F
from pyspark.sql import SparkSession, DataFrame
# from delta.tables import DeltaTable
import pyspark.sql.types as t
import pyspark
import spark

from typing import Tuple
import datetime
from datetime import timezone
from datetime import datetime, timezone
from typing import Optional
import pyspark.sql.functions as F
import pyspark.sql.types as T
from functools import reduce
from pyspark.sql import functions as f


def convert_schema():
    pass

def schemas_equal():
    pass

def ensure_sparksession():
    pass

def format_schema_name():
    pass

def union_dfs():
    pass





import numpy as np
def percentile_95(x):
    return float(np.percentile(x, 95))

def co2_average(*args):
    return float(np.average(list(args)))


def is_table_updated_ucatalog(table_name:str, df: pyspark.sql.DataFrame, key_cols: List[str]):
    # full_table_name = ".".join(list(format_table_name(table_name=table_name, database_name=database_name)))
    if spark.catalog.tableExists(table_name):
        logging.info(f'Checking in metadata table {table_name}...')
        current_table = {tuple(row[col] for col in key_cols) for row in df.collect()}
        saved_table = {
                       tuple(row[col] for col in key_cols) for row in 
                       spark.table(table_name)
                            .filter(F.col('is_obsolate') == False)
                            .select(*key_cols)
                            .distinct()
                            .collect()
                      }
        if len(saved_table.symmetric_difference(current_table)) == 0:
            logging.info(f'Table {table_name} have not been updated.')
            return False
    else:
        logging.info(f'{table_name} does not exist in Hive Metastore.')
    return True

def update_bronze_metadata_catalog(run_id, updated_metadata_df, bronze_metadata_table) : 
    if spark.catalog.tableExists(bronze_metadata_table):
        print(f'Obsolating old records if they exist for RUN_ID={run_id} in {bronze_metadata_table}')
        spark.sql(f"UPDATE {bronze_metadata_table} SET is_obsolate = true WHERE RUN_ID='{run_id}'")
        print(f"Metadata table {bronze_metadata_table} updated with obsolate entries.")
    else:
        print(f'Creating and writing new table {bronze_metadata_table}, RUN_ID={run_id}')
    print(f'Appending new records to {bronze_metadata_table}, RUN_ID={run_id}')
    updated_metadata_df.write.format("delta").mode("append").option("mergeSchema", "true").partitionBy("run_id").saveAsTable(bronze_metadata_table)

def save_to_table(
    df: pyspark.sql.DataFrame,
    table_name: str,
    save_mode: str = "overwrite",
    schema_option: Optional[str] = None,
    partition_cols: Optional[List[str]] = None,
    spark: Optional[pyspark.sql.SparkSession] = None,
):
 
    writer = _get_writer(df, save_mode, schema_option, partition_cols)
    params = {}
    writer.saveAsTable(table_name, **params)

def _get_writer(
    df: pyspark.sql.DataFrame,
    save_mode: str = "overwrite",
    schema_option: Optional[str] = None,
    partition_cols: Optional[List[str]] = None,
) -> pyspark.sql.DataFrameWriter:

    writer = df.write.format("delta")
    writer = writer.mode(save_mode)
    if schema_option:
        writer = writer.option(schema_option, True)
    if partition_cols:
        writer = writer.partitionBy(partition_cols)
    return writer

def delta_match_schema(
    reference_schema: Union[t.StructType, str],
    delta_table_name: str = "",
    df: DataFrame = None,
    spark: Optional[SparkSession] = None,
    log: Optional[Any] = logging,
) -> bool:
    """Verify if a delta table implements a reference schema.

    :param reference_schema: The reference schema of the delta table
    :param delta_path: The path to the delta table (Optional if a dataframe is given)
    :param df: The dataframe to check schema (Optional if delta_path is given)
    :param spark: The SparkSession
    :param log: The logger object (Optional)
    :return: True if the delta table match the reference schema, False otherwise.
    :raises ValueError: If none of the parameters `delta_path`, `df` or `reference_schema` are given.
    """
    # spark = ensure_sparksession(spark)
    if reference_schema:
        if isinstance(reference_schema, str):
            reference_schema = convert_schema(
                schema=reference_schema, output_format="struct_type"
            )
        if delta_table_name:
            delta_schema = spark.read.format("delta").table(delta_table_name).schema
        elif df:
            delta_schema = df.schema
        else:
            log.error("None of the parameters delta_path or df have been given.")
            raise ValueError("Delta schema not defined.")
        return reference_schema == delta_schema
    else:
        log.error("A reference schema has not been given.")
        raise ValueError(
            "A reference schema has not been given. "
            "Please, provide a reference_schema to check the dataframe schema against."
        )

def ensure_delta_exists(
    delta_table_name: str,
    reference_schema: Union[t.StructType, str],
    df: Optional[pyspark.sql.DataFrame] = None,
    partition_cols: Optional[Union[List[str], str]] = None,
    spark: Optional[SparkSession] = None,
    log: Optional[Any] = logging,
):

    # spark = ensure_sparksession(spark)
    log.info(
        f"{delta_table_name} delta table does not exist. Creation of the delta table in {delta_table_name}..."
    )
    if df is None:
        df = spark.createDataFrame(data=[], schema=reference_schema)
    else:
        if not schemas_equal(
            df.schema,
            convert_schema(schema=reference_schema, output_format="struct_type"),
        ):
            log.warning(
                "The schema of the input dataframe does not match the given reference schema."
            )
            log.info("Enforcing the reference schema to the input dataframe...")
            df = union_dfs(
                dfs=[spark.createDataFrame(data=[], schema=reference_schema), df],
                merge_schema=True,
                reference_schema=reference_schema,
            )

def table_exists(
    table_name: str,
    spark: Optional[SparkSession] = None,
) -> bool:

    # spark = ensure_sparksession(spark)
    if spark is None:
        spark = SparkSession.builder.appName("MyApp").getOrCreate()

    catalog_name, schema_name = format_schema_name(table_name)

    spark.sql(f"USE CATALOG {catalog_name}")
    if not schema_exists(schema_name, spark):
        return False
    if not spark.catalog.tableExists(table_name.lower()):
        return False
    return True

def schema_exists(schema_name: str, spark: Optional[SparkSession] = None) -> bool:

    # spark = ensure_sparksession(spark)
    if spark.catalog.databaseExists(schema_name.lower()):
        return True
    return False


def get_table_location(
    table_name: str,
    spark: Optional[SparkSession] = None,
) -> str:

    spark = ensure_sparksession(spark)
    return spark.sql(f"DESCRIBE DETAIL {table_name}").select("location").first()[0]

def format_table_name(
    table_name: str, database_name: str = "default", log: Optional[Any] = logging
) -> Tuple[str, str]:
    """Provide an additional verification on the hive table name format and correct it if necessary.

    :param table_name: The table name. A Database name prefix can be passed too, in the form of `dbname.table_name`
    :param database_name: The database name (Optional)
    :param log: The logger object (Optional)
    :return: Corrected format for the hive table name and database name.
    :raises ValueError: If the `database_name` is different from the one in `table_name`.
    """
    if "." in table_name:
        if database_name == "default" or database_name == table_name.split(".", 1)[0]:
            database_name, table_name = table_name.split(".", 1)
        else:
            exception_message = (
                f"Table name contains `.` but database name is explicitly defined. "
                f"Table name: {table_name}, database_name: {database_name}"
            )
            log.exception(exception_message)
            raise ValueError(exception_message)
    if any(d.isupper() for d in database_name) or any(t.isupper() for t in table_name):
        log.warning(
            "Beware that there are upper case characters in the database name or the table name."
            "Database name and table name will be return as lowercase to respect Hive Metastore names format."
        )
    return database_name.lower(), table_name.lower()

def forName(*args, **kwargs):
    """Create a DeltaTable using the given table or view name using the given SparkSession."""
    raise NotImplementedError(
        "DeltaTable class is only available on Databricks"
    )

def concat_columns(
    columns: List[str],
    *,
    sep: str = "_",
    source: Optional[Union[pyspark.sql.DataFrame, str]] = None,
) -> Union[pyspark.sql.Column, str]:

    if source is None:
        return F.concat_ws(sep, *[F.col(column_name) for column_name in columns])
    elif isinstance(source, pyspark.sql.DataFrame):
        return F.concat_ws(sep, *[source[column_name] for column_name in columns])
    elif isinstance(source, str):
        # Using the backticks around the columns name to encapsulate special characters in names
        columns = ", ".join(f"{source}.`{column_name}`" for column_name in columns)
        return f"CONCAT_WS('{sep}', {columns})"

def add_scd_columns(
        df: pyspark.sql.DataFrame,
        *,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
        is_current: bool = True,
        start_time_name: str = "start_time",
        end_time_name: str = "end_time",
        is_current_name: str = "is_current",
    ) -> pyspark.sql.DataFrame:

        if start_time is None:
            start_time = datetime(1970, 1, 1, tzinfo=timezone.utc)
        scd_df = (
            df.withColumn(start_time_name, F.lit(start_time).cast(T.TimestampType()))
            .withColumn(end_time_name, F.lit(end_time).cast(T.TimestampType()))
            .withColumn(is_current_name, F.lit(is_current))
        )
        return scd_df

def correct_scd_columns(
        df: pyspark.sql.DataFrame,
        *,
        start_time_name: str = "start_time",
        end_time_name: str = "end_time",
        is_current_name: str = "is_current",
    ) -> pyspark.sql.DataFrame:
        df = (
            df.withColumn(
                start_time_name,
                F.when(
                    F.col(start_time_name).isNull(),
                    F.lit(datetime(1970, 1, 1, tzinfo=timezone.utc)),
                ).otherwise(F.col(start_time_name)),
            )
            # Correcting the null values in is_current to true: when there is no end-time
            .withColumn(
                is_current_name,
                F.when(
                    (F.col(is_current_name).isNull())
                    & (
                        (F.col(end_time_name).isNull())
                        | (F.col(end_time_name) > F.lit(datetime.now()))
                    ),
                    F.lit(True),
                )
                .when(
                    (F.col(is_current_name).isNull())
                    & (F.col(end_time_name) < F.lit(datetime.now())),
                    F.lit(False),
                )
                .otherwise(F.col(is_current_name)),
            )
        )
        return df

def validate_scd_columns(
        df: pyspark.sql.DataFrame,
        *,
        start_time_name: str = "start_time",
        end_time_name: str = "end_time",
        is_current_name: str = "is_current",
    ) -> pyspark.sql.DataFrame:

        scd_cols = [start_time_name, end_time_name, is_current_name]
        if set(scd_cols).issubset(set(df.columns)):  # early stopping
            return SCDManager.correct_scd_columns(
                df,
                start_time_name=start_time_name,
                end_time_name=end_time_name,
                is_current_name=is_current_name,
            )

        if start_time_name not in df.columns:
            logging.info(
                f"Beware that there is no column `{start_time_name}` in the dataframe.\n"
                f"Initializing the column `{start_time_name}` to the origin date 01-01-1970..."
            )
            df = df.withColumn(
                start_time_name, F.lit(datetime(1970, 1, 1, tzinfo=timezone.utc))
            )
        if end_time_name not in df.columns:
            logging.info(
                f"Beware that there is no column `{end_time_name}` in the dataframe.\n"
                f"Adding `{end_time_name}` column values depending on the values of `{is_current_name}`..."
            )
            if is_current_name in df.columns:
                df = df.withColumn(
                    end_time_name,
                    F.when(
                        F.col(is_current_name) == F.lit(True),
                        F.lit(None).cast(T.TimestampType()),
                    ).when(
                        F.col(is_current_name) == F.lit(False),
                        F.lit(datetime.now(timezone.utc)).cast(T.TimestampType()),
                    ),
                )
            else:
                df = df.withColumn(end_time_name, F.lit(None).cast(T.TimestampType()))
        if is_current_name not in df.columns:
            logging.info(
                f"Beware that there is no column `{is_current_name}` in the dataframe.\n"
                f"Adding `{is_current_name}` column values depending on the values of `{end_time_name}`..."
            )
            if end_time_name in df.columns:
                df = df.withColumn(
                    is_current_name,
                    F.when(F.col(end_time_name).isNull(), F.lit(True)).when(
                        F.col(end_time_name).isNotNull(), F.lit(False)
                    ),
                )
            else:
                df = df.withColumn(is_current_name, F.lit(True))
        return SCDManager.correct_scd_columns(
            df,
            start_time_name=start_time_name,
            end_time_name=end_time_name,
            is_current_name=is_current_name,
        ).select(
            [column_name for column_name in df.columns if column_name not in scd_cols]
            + scd_cols
        )

class SCDManager:
    """Manager Class for Slowly Changing Dimension Tables."""

    def __init__(
        self,
        spark: pyspark.sql.SparkSession,
        table_name: str,
        id_cols: Union[str, List[str]],
        start_time_name: str = "start_time",
        end_time_name: str = "end_time",
        is_current_name: str = "is_current",
    ):

        self.spark = spark
        self.table_name = table_name
        if not isinstance(id_cols, (list,)):
            id_cols = [id_cols]
        self.id_cols = id_cols
        self.start_time = start_time_name
        self.end_time = end_time_name
        self.is_current = is_current_name
        # The column names to consider as SCD columns.
        self.scd_cols = [self.start_time, self.end_time, self.is_current]

def SCD(
    spark: pyspark.sql.SparkSession,
    df: pyspark.sql.DataFrame,
    table_name: str,
    table_path: str,
    id_cols: Union[str, List[str]],
    *,
    partition_cols: Optional[List[str]] = None,
    start_time_name: str = "start_time",
    end_time_name: str = "end_time",
    is_current_name: str = "is_current",
):
    """Apply all the SCD logic to a new updated Spark Dataframe and save in table.

    The updated dataframe `df` does not need to have SCD columns start_time, end_time and is_current.
    If it is the case their values will be overwritten to reflect the real time when the updated rows were added to the
    SCD table.

    :param spark: The Spark Session to use for the SCDManager
    :param df: The updated Spark Dataframe to merge into the existing table
    :param table_name: The delta table name
    :param id_cols: The id columns in the delta table
    :param partition_cols: The values to partition the data by. (Optional)
                           In the application of SCD in metadata, the "is_current" column is generally used as
                           partitioning column
    :param start_time_name: The SCD start time column name. Default start_time.
    :param end_time_name: The SCD end time column name. Default start_time.
    :param is_current_name: The SCD status column name. Default is_current.
    """
    scdm = SCDManager(
        spark=spark,
        table_name=table_name,
        id_cols=id_cols,
        start_time_name=start_time_name,
        end_time_name=end_time_name,
        is_current_name=is_current_name,
    )

    if not table_exists(table_name=table_name, spark=spark):
        logging.info(f"Creating and writing new table {table_name}.")
        # Adding the scd columns with their default values
        df = SCDManager.add_scd_columns(
            df,
            start_time_name=start_time_name,
            end_time_name=end_time_name,
            is_current_name=is_current_name,
        )
        save_to_table(
            df=df,
            table_name=table_name,
            save_mode="overwrite",
            schema_option="mergeSchema",
            partition_cols=partition_cols,
        )
        df.write.format("delta").mode("overwrite").option("mergeSchema", "true").save(table_path)
    else:
        logging.info(f"SCD Merging new data into table {table_name}...")

        # If the delta table exist but doesn't have all the SCD columns, we add them
        if not set(scdm.scd_cols).issubset(set(spark.table(table_name).columns)):
            updated_saved_df = SCDManager.validate_scd_columns(
                spark.table(table_name),
                start_time_name=start_time_name,
                end_time_name=end_time_name,
                is_current_name=is_current_name,
            )
            save_to_table(
                df=updated_saved_df,
                table_name=table_name,
                save_mode="overwrite",
                schema_option="mergeSchema",
                partition_cols=partition_cols,
            )
            updated_saved_df.write.format("delta").mode("overwrite").option("mergeSchema", "true").save(table_path)

        # If updated spark dataframe contains SCD columns, we drop them
        for scd_col in scdm.scd_cols:
            if scd_col in df.columns:
                logging.warning(
                    f"""Beware that with the SCD function, the updated dataframe does not need to have SCD columns {scd_col}.
Their values will be automatically overwritten by the SCD function to reflect the real time when the updated rows were added to the SCD table.
If this is not a desired functionality and you want to keep the scd columns in your updated dataframes, please use directly the SCDManager.merge method."""
                )
                df = df.drop(scd_col)

        # We set a start date column for the SCD:
        # - for any new measurement: UTC start time (1970-01-01)
        # - for any measurement that is already in the delta tables: the actual timestamp
        # We set the start time of metadata to a past value to make sure historical time series queries
        # have a metadata to match.
        df_with_ids_to_update = (
            spark.table(table_name)
            .select([F.col(id_col).alias("updated_" + id_col) for id_col in id_cols])
            .distinct()
            .withColumn(start_time_name, F.lit(datetime.now()).cast(T.TimestampType()))
        )
        updated_df = (
            F.broadcast(df)
            .join(
                df_with_ids_to_update,
                on=[
                    df[id_col] == df_with_ids_to_update["updated_" + id_col]
                    for id_col in id_cols
                ],
                how="left",
            )
            .fillna({start_time_name: datetime(1970, 1, 1).isoformat()})
            .drop(*["updated_" + id_col for id_col in id_cols])
        )
        scdm.merge(df=updated_df, has_start_time=True)

def columns_different(
    col1: pyspark.sql.Column, col2: pyspark.sql.Column
) -> pyspark.sql.Column:
    """Verify if two columns are different or not both null.

    :param col1: The first column to compare with
    :param col2: The second column to compare with
    :return: The boolean pyspark column
    """
    clear_diff = (
        (col1 != col2)
        | (col1.isNull() & col2.isNotNull())
        | (col1.isNotNull() & col2.isNull())
    )
    return F.when(clear_diff.isNull(), F.lit(False)).otherwise(clear_diff)

def merge_expressions(
    expressions: List[pyspark.sql.Column], operation: str
) -> pyspark.sql.Column:
    """Apply an operation between multiple columns.

    For the moment, the supported operations are OR, AND and XOR.

    :param expressions: The list of pyspark columns to apply the operation on
    :param operation: The operation to apply
    :return: The resulting pyspark column
    :raises ValueError: When the unknown operation parameter is provided
    """
    if len(expressions) == 1:
        return expressions[0]

    valid_operations = {"AND", "OR"}
    unsupported_operations = {"NAND", "NOR", "XAND", "XOR", "XNAND", "XNOR"}

    if operation.strip().upper() not in valid_operations:
        status = (
            "not supported yet by the merge_expressions function"
            if operation.strip().upper() in unsupported_operations
            else "not recognised"
        )
        raise ValueError(
            f"The parameter operation={operation} is {status}. "
            f"Please use an operation among {valid_operations}."
        )

    operation_mapping = {
        "OR": lambda col1, col2: col1 | col2,
        "AND": lambda col1, col2: col1 & col2,
    }
    return reduce(operation_mapping[operation.strip().upper()], expressions)


def format_value(column_value):
    if isinstance(column_value, bool):
        if column_value is True:
            return "true"
        elif column_value is False:
            return "false"
        else:
            return ""
    elif isinstance(column_value, list):
        return "_".join([str(val) for val in column_value])
    elif isinstance(column_value, datetime):
        return column_value.strftime("%Y-%m-%d %H:%M:%S")
    else:
        return str(column_value)
    

