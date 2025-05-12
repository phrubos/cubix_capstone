import pyspark.sql.functions as sf
from pyspark.sql import DataFrame

CUSTOMERS_MAPPING = {
    "ck": "CustomerKey",
    "name": "Name",
    "bdate": "BirthDate",
    "ms": "MaritalStatus",
    "gender": "Gender",
    "income": "YearlyIncome",
    "childrenhome": "NumberChildrenAtHome",
    "occ": "Occupation",
    "hof": "HouseOwnerFlag",
    "nco": "NumberCarsOwned",
    "addr1": "Addressline1",
    "addr2": "Addressline2",
    "phone": "Phone"
}


def get_customers(customers_raw: DataFrame) -> DataFrame:
    """
    Transform the raw customers data into a structured format.

    1. select the relevant columns from the raw customers data.
    2. rename the columns to match the target schema.
    3. transform MarialStatus and Gender columns to integers.
    4. transform the BirthYear column to integers.
    5. transform the IncomeCategory column to integers.
    6. transform the FullAddress column to a string.
    7. drop duplicates from the data.

    Args:
        customers_raw (DataFrame): The raw customers data.

    Returns:
        DataFrame: The transformed customers data.
    """
    # First select and cast columns
    df = customers_raw.select(
        sf.col("ck").cast("int"),
        sf.col("name"),
        sf.col("bdate").cast("date"),
        sf.col("ms"),
        sf.col("gender"),
        sf.col("income").cast("int"),
        sf.col("childrenhome").cast("int"),
        sf.col("occ"),
        sf.col("hof").cast("int"),
        sf.col("nco").cast("int"),
        sf.col("addr1"),
        sf.col("addr2"),
        sf.col("phone"),
    )

    # Rename columns using a loop instead of withColumnsRenamed
    for old_name, new_name in CUSTOMERS_MAPPING.items():
        df = df.withColumnRenamed(old_name, new_name)

    # Apply transformations
    df = df.withColumn(
        "MaritalStatus",
        sf.when(sf.col("MaritalStatus") == "M", 1)
          .when(sf.col("MaritalStatus") == "S", 0)
          .otherwise(None)
          .cast("int")
    )

    df = df.withColumn(
        "Gender",
        sf.when(sf.col("Gender") == "M", 1)
          .when(sf.col("Gender") == "F", 0)
          .otherwise(None)
          .cast("int")
    )

    df = df.withColumn(
        "FullAdress",
        sf.concat_ws(",", sf.col("Addressline1"), sf.col("Addressline2"))
    )

    df = df.withColumn(
        "IncomeCategory",
        sf.when(sf.col("YearlyIncome") <= 50000, "Low")
          .when((sf.col("YearlyIncome") <= 100000), "Medium")
          .otherwise("High")
    )

    df = df.withColumn(
        "BirthYear",
        sf.year(sf.col("BirthDate")).cast("int")
    )

    # Drop duplicates
    return df.dropDuplicates()
