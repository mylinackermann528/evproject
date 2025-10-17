from pyspark.sql.functions import col, explode
from pyspark.sql import DataFrame

def clean_ev_data(df: DataFrame) -> DataFrame:
    column_metadata = df.select("meta.view.columns").first()[0]
    column_names = [field['fieldName'] for field in column_metadata]

    df_exploded = df.select(explode(col("data")).alias("record"))

    select_expressions = [col("record")[i].alias(column_names[i]) for i in range(len(column_names))]
    final_df = df_exploded.select(select_expressions)

    final_df = final_df.withColumn("model_year", col("model_year").cast("integer")) \
                       .withColumn("electric_range", col("electric_range").cast("integer")) \
                       .withColumn("base_msrp", col("base_msrp").cast("integer")) \
                       .withColumn("legislative_district", col("legislative_district").cast("integer")) \
                       .withColumn("dol_vehicle_id", col("dol_vehicle_id").cast("integer"))
                       
    final_df = final_df.filter(col("vin_1_10").isNotNull())

    return final_df
