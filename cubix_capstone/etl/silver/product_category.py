import pyspark.sql.functions as sf
from pyspark.sql import DataFrame

PRODUCT_CATEGORY_MAPPING = {
"pck": "ProductCategoryKey",
"espn": "EnglishProductSubcategoryName",
"spsn": "SpanishProductSubcategoryName",
"fpsn": "FrenchProductSubcategoryName"
}


def get_product_category(product_category_raw: DataFrame) -> DataFrame:
    """
    Transform the raw product category data into a structured format.

    1. select the relevant columns from the raw product category data.
    2. rename the columns to match the target schema.

    """

    return (
            product_category_raw
            .select(
            sf.col("pck").cast("int"),
            sf.col("espn"),
            sf.col("spsn"),
            sf.col("fpsn")
            )
            .withColumnsRenamed(PRODUCT_CATEGORY_MAPPING)
            .dropDuplicates()
    )