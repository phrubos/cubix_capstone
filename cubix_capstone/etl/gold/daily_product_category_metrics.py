import pyspark.sql.functions as sf
from pyspark.sql import DataFrame

def get_daily_product_category_metrics(wide_sales: DataFrame) -> DataFrame:
    """

    """

    return (
        wide_sales
        .groupBy(sf.col("EnglishProductCategoryName"), sf.col("OrderDate"))
        .agg(
            sf.sum(sf.col("SalesAmount")).alias("SalesAmountSum"),
            sf.round(sf.avg(sf.col("SalesAmount")), 2).alias("SalesAmountAvg"),
            sf.sum(sf.col("Profit")).alias("ProfitSum"),
            sf.round(sf.avg(sf.col("Profit")), 2).alias("ProfitAvg"),
        )
    )
