from pyspark.sql import DataFrame
from pyspark.sql.functions import col, isnull


def assert_no_nulls(df: DataFrame, columns: list[str]) -> DataFrame:
    """Filter out rows with nulls in critical columns and log the drop count."""
    total = df.count()
    condition = ~isnull(col(columns[0]))
    for c in columns[1:]:
        condition = condition & ~isnull(col(c))
    clean_df = df.filter(condition)
    dropped = total - clean_df.count()
    if dropped > 0:
        print(f"[validation] Dropped {dropped} rows with nulls in {columns}")
    return clean_df


def assert_valid_event_types(df: DataFrame, allowed: list[str]) -> DataFrame:
    """Filter rows whose event_type is not in the allowed set."""
    return df.filter(col("event_type").isin(allowed))
