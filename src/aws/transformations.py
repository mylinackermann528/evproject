from pyspark.sql.functions import col, explode
from pyspark.sql import DataFrame

def clean_ev_data(df: DataFrame, columns: list) -> DataFrame:
    if "data" in df.columns:
        df_exploded = df.select(explode(col("data")).alias("record"))
    else:
        df_exploded = df.alias("record")

    select_expressions = []
    for i, column_name in enumerate(columns):
        select_expressions.append(col(f"record.{i}").alias(column_name))
        
    final_df = df_exploded.select(select_expressions)

    final_df = final_df.withColumn("Model_Year", col("Model_Year").cast("integer")) \
                       .withColumn("Electric_Range", col("Electric_Range").cast("integer"))
                       
    final_df = final_df.filter(col("VIN_1_10").isNotNull())

    final_df = final_df.select(*columns)

    return final_df
