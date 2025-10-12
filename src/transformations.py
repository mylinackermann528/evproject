# Example function
def clean_ev_data(df, columns):
    # The data is in a single column named 'data' which is an array of arrays
    from pyspark.sql.functions import col, explode

    # Explode the top-level array
    df_exploded = df.select(explode(col("data")).alias("record"))

    # Select and cast columns dynamically based on the provided list
    for i, column_name in enumerate(columns):
        df_exploded = df_exploded.withColumn(column_name, col("record")[i])

    # Drop the intermediate record column
    final_df = df_exploded.drop("record")

    # Add data quality steps: cast types, handle nulls, etc.
    final_df = final_df.withColumn("model_year", col("model_year").cast("integer")) \
                       .withColumn("electric_range", col("electric_range").cast("integer"))

    return final_df
