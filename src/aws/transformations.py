from pyspark.sql.functions import col
from pyspark.sql import DataFrame

def clean_ev_data(df: DataFrame, columns: list) -> DataFrame:
    final_df = df

    final_df = final_df.withColumn("Model_Year", col("Model_Year").cast("integer")) \
                       .withColumn("Electric_Range", col("Electric_Range").cast("integer"))
                       
    final_df = final_df.filter(col("VIN_1_10").isNotNull())

    final_df = final_df.select(*columns)

    return final_df
