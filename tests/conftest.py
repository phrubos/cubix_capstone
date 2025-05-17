from pyspark.sql import SparkSession, DataFrame
from pytest import fixture

SPARK = (
    SparkSession
    .builder
    .appName("localTests")
    .master("local")
    .getOrCreate()  
)

@fixture()
def spark():
    return SPARK.getActiveSession()

@fixture()
def some_df()-> DataFrame:
    return SPARK.createDataFrame(
        [
            ("some_data",)
        ],
        schema = ["col1",]
    )
