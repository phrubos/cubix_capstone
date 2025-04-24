from pyspark.sql import SparkSession, DataFrame
from src.utils.config import STORAGE_ACCOUNT_NAME


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

    file_path = f"abfss://{container_name}@{STORAGE_ACCOUNT_NAME}.dfs.core.windows.net/{file_path}"


    spark = SparkSession.getActiveSession()

    if not spark:
        raise RuntimeError("Spark session is not active")


    if format == "json":
        df = spark.read.json(file_path)
        return df
    
    else:

        df = (
        spark
        .read
        .format(format)
        .option("header", "true")
        .load(file_path, format = format)
            )

    return df