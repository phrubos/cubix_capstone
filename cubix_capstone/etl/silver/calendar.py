import pyspark.sql.functions as sf
from pyspark.sql import DataFrame


def get_calendar(calendar_raw: DataFrame) -> DataFrame:
    """
    Transform the raw calendar data into a structured format.

    Args:
        calendar_raw (DataFrame): The raw calendar data.

    Returns:
        DataFrame: The transformed calendar data.
    """



    return (
    calendar_raw
    .select(
        sf.col("Date").cast("date"),
        sf.col("DayNumberOfWeek").cast("int"),
        sf.col("DayName"),
        sf.col("MonthName"),
        sf.col("MonthNumberOfYear").cast("int"),
        sf.col("DayNumberOfYear").cast("int"),
        sf.col("WeekNumberOfYear").cast("int"),
        sf.col("CalendarQuarter").cast("int"),
        sf.col("CalendarYear").cast("int"),
        sf.col("FiscalYear").cast("int"),
        sf.col("FiscalSemester").cast("int"),
        sf.col("FiscalQuarter").cast("int"),
        sf.col("FinMonthNumberOfYear").cast("int"),
        sf.col("DayNumberOfMonth").cast("int"),
        sf.col("MonthID").cast("int"),
    )
    .dropDuplicates()

    )

    