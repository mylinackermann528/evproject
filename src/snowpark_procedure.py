from snowflake.snowpark import Session
from snowflake.snowpark.functions import col, flatten
from snowflake.snowpark.types import StringType, IntegerType

from data_quality import run_dq_checks

def main(session: Session):
    print("Starting server-side EV data transformation with Snowpark...")

    raw_df = session.table("raw_ev_data")

    print("Discovering schema from JSON metadata...")
    column_metadata_df = raw_df.select(flatten(raw_df['"RAW_JSON"']['meta']['view']['columns']))
    
    column_names_rows = column_metadata_df.select(col("VALUE")['fieldName'].alias("name")).collect()
    column_names = [row['name'] for row in column_names_rows]
    print(f"Discovered {len(column_names)} columns: {column_names}")

    df_flattened = raw_df.flatten(raw_df['"RAW_JSON"']['data'])
    df_exploded = df_flattened.select(col("VALUE").alias("record"))
    exprs = [col("record")[i].alias(column_names[i]) for i in range(len(column_names))]
    parsed_df = df_exploded.select(*exprs)

    final_column_mapping = {
        "vin_1_10": ("VIN", StringType()),
        "city": ("City", StringType()),
        "state": ("State", StringType()),
        "make": ("Make", StringType()),
        "model": ("Model", StringType()),
        "model_year": ("ModelYear", IntegerType()),
        "ev_type": ("EV_Type", StringType()),
        "electric_range": ("ElectricRange", IntegerType()),
        "base_msrp": ("BaseMSRP", IntegerType())
    }

    final_select_exprs = []
    for column_name in parsed_df.columns:
        if column_name in final_column_mapping:
            alias, new_type = final_column_mapping[column_name]
            final_select_exprs.append(col(column_name).cast(new_type).alias(alias))
        else:
            final_select_exprs.append(col(column_name))

    final_df = parsed_df.select(*final_select_exprs)

    dq_results = run_dq_checks(final_df)

    if dq_results["null_vin_count"] > 0:
        raise ValueError(f"Critical DQ Check Failed: {dq_results['null_vin_count']} Null VINs found. Halting pipeline.")
    
    if dq_results["zero_msrp_count"] > 0:
        print(f"DQ Warning: Found {dq_results['zero_msrp_count']} records with a Base MSRP of 0.")

    final_df.write.mode("overwrite").save_as_table("clean_ev_data_snowpark")
    
    return "Transformation complete. Schema detected dynamically. Data successfully saved."

