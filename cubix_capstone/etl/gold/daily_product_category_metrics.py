import pyspark.sql.functions as sf
from pyspark.sql import DataFrame

def get_daily_product_category_metrics(wide_sales: DataFrame) -> DataFrame:
    """
    Calculate daily product category metrics.
    1. Group by EnglishProductCategoryName.
    2. Calculate the following metrics:
        - SalesAmountSum: sum of SalesAmount
        - SalesAmountAvg: average of SalesAmount
        - ProfitSum: sum of Profit
        - ProfitAvg: average of Profit

    Parameters
    ----------
    wide_sales : DataFrame
        DataFrame containing the wide sales data.

    Returns
    -------
    DataFrame
        DataFrame containing the daily product category metrics.
    """

    return (
        wide_sales
        .groupBy(sf.col("EnglishProductCategoryName"))
        .agg(
            sf.sum(sf.col("SalesAmount")).alias("SalesAmountSum"),
            sf.round(sf.avg(sf.col("SalesAmount")), 2).alias("SalesAmountAvg"),
            sf.sum(sf.col("Profit")).alias("ProfitSum"),
            sf.round(sf.avg(sf.col("Profit")), 2).alias("ProfitAvg"),
        )
    )
