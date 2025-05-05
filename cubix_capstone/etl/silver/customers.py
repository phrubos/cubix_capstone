import pyspark.sql.functions as sf
from pyspark.sql import DataFrame

CUSTOMERS_MAPPING = {
"ck" :	"CustomerKey"
"name":	"Name"
"bdate":	"BirthDate"
"ms"	:"MaritalStatus"
"gender":	"Gender"
"income":	"YearlyIncome"
"childrenhome":	"NumberChildrenAtHome"
"occ":	"Occupation"
"hof":	"HouseOwnerFlag"
"nco":	"NumberCarsOwned"
"addr1":	"Addressline1"
"addr2":	"Addressline2"
"phone":	"Phone"

}


def get_customers(customers_raw: DataFrame) -> DataFrame:


    """
    1. select the relevant columns from the raw customers data.
    2. rename the columns to match the target schema.
    3. transform MarialStatus and Gender columns to integers.
    4. transform the BirthYear column to integers.
    5. transform the IncomeCategory column to integers.
    6. transform the FullAddress column to a string.
    7. drop duplicates from the data.



    Transform the raw customers data into a structured format.

    Args:
        customers_raw (DataFrame): The raw customers data.

    Returns:
        DataFrame: The transformed customers data.
    """

    return (
        customers_raw
        .select(
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
        .withColumnsRenamed(CUSTOMERS_MAPPING)
        .withColumn("MaritalStatus", sf.when(sf.col("MaritalStatus") == "M", 1)
        .when(sf.col("MaritalStatus") == "S", 0)
        .otherwise(None))
        .cast("int")
        .withColumn("Gender", sf.when(sf.col("Gender") == "M", 1)
        .when(sf.col("Gender") == "F", 0)
        .otherwise(None))
        .cast("int")
        .withColumn("FullAdress",
         sf.concat(", ",sf.col("AddressLine1"), sf.col("AddressLine2")
         ))
         .withColumn(
            "IncomeCategory",
            sf.when(sf.col("YearlyIncome") <= 50000, "Low")
            .when((sf.col("YearlyIncome") <= 100000), "Medium")
            .otherwise("High")
         )
         .withColumn(
            "BirthYear",
            sf.year(sf.col("BirthDate"))
            .cast("int")

    )
    .dropDuplicates()
    )
