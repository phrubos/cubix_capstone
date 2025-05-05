from pyspark.sql import SparkSession
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