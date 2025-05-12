import pyspark.sql.functions as sf
from pyspark.sql import DataFrame

PRODUCT_SUBCATEGORY_MAPPING = {
"psk": "ProductSubcategoryKey",
"pck": "ProductCategoryKey",
"espn": "EnglishProductSubcategoryName",
"spsn": "SpanishProductSubcategoryName",
"fpsn": "FrenchProductSubcategoryName"
}


def get_product_subcategory(product_subcategory_raw: DataFrame) -> DataFrame:
    """
    Transform the raw product subcategory data into a structured format.

    1. select the relevant columns from the raw product subcategory data.
    2. rename the columns to match the target schema.

    """

    return (
            product_subcategory_raw
            .select(
            sf.col("psk").cast("int"),
            sf.col("pck").cast("int"),
            sf.col("espn"),
            sf.col("spsn"),
            sf.col("fpsn")
            )
            .withColumnsRenamed(PRODUCT_SUBCATEGORY_MAPPING)
            .dropDuplicates()
    )