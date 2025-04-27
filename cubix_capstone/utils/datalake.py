from pyspark.sql import SparkSession, DataFrame
from cubix_capstone.utils.config import STORAGE_ACCOUNT_NAME


def read_file_from_datalake(container_name: str, file_path: str, format: str)-> DataFrame:

    """
    Function to read a file from the datalake
    Args:

    container_name: str: The name of the container in the datalake
    file_path: str: The path to the file in the datalake
    format: str: The format of the file (csv, parquet, etc.)

    returns:
    df: DataFrame: The dataframe containing the data from the file

    """

    if format not in ["csv", "parquet", "json", "delta"]:
        raise ValueError("format must be one of csv, parquet, json, or delta")

    full_path = f"abfss://{container_name}@{STORAGE_ACCOUNT_NAME}.dfs.core.windows.net/{file_path}"


    spark = SparkSession.getActiveSession()

    if not spark:
        raise RuntimeError("Spark session is not active")


    if format == "json":
        df = spark.read.json(full_path)
        return df

    else:

        df = (
        spark
        .read
        .format(format)
        .option("header", "true")
        .load(full_path, format = format)
            )

    return df



def write_file_to_datalake(
    df: DataFrame,
    container_name: str,
    file_path: str,
    format: str,
    mode: str = "overwrite",
    partition_by: list[str] = None)-> None:

    """
    Function to write a file to the datalake
    Args:

    container_name: str: The name of the container in the datalake
    file_path: str: The path to the file in the datalake
    df: DataFrame: The dataframe containing the data to be written to the file
    format: str: The format of the file (csv, parquet, etc.)
    mode: str: The mode to write the file (overwrite, append, etc.)
    partition_by: list[str]: The columns to partition the file by


    """

    if format not in ["csv", "parquet", "delta"]:
        raise ValueError("format must be one of csv, parquet, or delta")



    full_path = f"abfss://{container_name}@{STORAGE_ACCOUNT_NAME}.dfs.core.windows.net/{file_path}"

    writer = df.write.mode(mode).format(format)

    if format == "csv":
        writer = writer.option("header", "true")

    if partition_by:
        writer = writer.partitionBy(*partition_by)

    writer.save(full_path)
