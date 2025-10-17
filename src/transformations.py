from pyspark.sql.functions import col, explode
from pyspark.sql import DataFrame

# Example function to perform JSON flattening, column selection, and quality checks
# Assumes the JSON file structure is a top-level array, or a single record with an array-of-arrays field named 'data'
def clean_ev_data(df: DataFrame, columns: list) -> DataFrame:
    """
    Reads the raw data (assumed to be nested), flattens it, selects required columns
    positionally, and applies basic data quality checks.

    :param df: The raw DataFrame loaded from S3.
    :param columns: List of final column names to select (restricts new columns).
    :return: The cleaned DataFrame (Silver/Curated layer).
    """

    # --- 1. Flattening (Based on initial array-of-arrays structure) ---
    # Explode the top-level array, assuming the raw JSON structure required this
    if "data" in df.columns:
        df_exploded = df.select(explode(col("data")).alias("record"))
    else:
        # If 'data' column is not found, assume data is already in individual records (unexpected but safe fallback)
        df_exploded = df.alias("record")

    # --- 2. Dynamic Selection and Extraction (Schema Restriction) ---
    # Dynamically select and cast columns based on the provided list (COLUMN_NAMES)
    select_expressions = []
    for i, column_name in enumerate(columns):
        # We extract the column by position (index i) from the 'record' array/struct
        select_expressions.append(col(f"record.{i}").alias(column_name))
        
    final_df = df_exploded.select(select_expressions)

    # --- 3. Data Quality & Casting ---
    # Apply type casting and basic quality measures
    final_df = final_df.withColumn("Model_Year", col("Model_Year").cast("integer")) \
                       .withColumn("Electric_Range", col("Electric_Range").cast("integer"))
                       
    # Simple Quality Check: Remove rows where critical identifiers are null
    final_df = final_df.filter(col("VIN_1_10").isNotNull())


    # --- 4. Schema Restriction Enforcement ---
    # The selection in step 2 inherently restricts new columns. We ensure the order is correct.
    final_df = final_df.select(*columns)

    return final_df
