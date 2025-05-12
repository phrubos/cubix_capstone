import pyspark.sql.functions as sf
from pyspark.sql import DataFrame
from pyspark.sql.types import DecimalType

PRODUCTS_MAPPING = {
  "pk": "ProductKey",
  "psck": "ProductSubCategoryKey",  # Changed from "p1" to "psck"
  "name": "ProductName",
  "stancost": "StandardCost",
  "dealerprice": "DealerPrice",
  "listprice": "ListPrice",
  "color": "Color",
  "size": "Size",
  "range": "SizeRange",
  "weight": "Weight",
  "nameofmodel": "ModelName",
  "ssi": "SafetyStockLevel",  # Assuming this is the correct mapping
  "desc": "Description"
}


def get_products(products_raw: DataFrame) -> DataFrame:
    """
    Transform the raw products data into a structured format.

    1. select the relevant columns from the raw products data.
    2. rename the columns to match the target schema.
    3. Create a new column "ProfitMargin" as the difference between ListPrice and DealerPrice.
    4. replace "NA" with None in the data.
    5. drop duplicates from the data.

    """

    return (
            products_raw
            .select(
            sf.col("pk").cast("int"),
            sf.col("psck").cast("int"),
            sf.col("name"),
            sf.col("stancost").cast(DecimalType(10, 2)).alias("stancost"),
            sf.col("dealerprice").cast(DecimalType(10, 2)).alias("dealerprice"),
            sf.col("listprice").cast(DecimalType(10, 2)).alias("listprice"),
            sf.col("color"),
            sf.col("size").cast("int"),
            sf.col("range"),
            sf.col("weight").cast(DecimalType(10, 2)).alias("weight"),
            sf.col("nameofmodel"),
            sf.col("ssi").cast("int"),  # Changed from "ssl" to "ssi"
            sf.col("desc")
            )
            .withColumnsRenamed(PRODUCTS_MAPPING)
            .withColumn("ProfitMargin", sf.col("ListPrice") - sf.col("DealerPrice"))
            .replace("NA", None)
            .dropDuplicates()
    )
