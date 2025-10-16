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
    
    column_names = [row['NAME'] for row in column_names_rows]
    print(f"Discovered {len(column_names)} columns.")

    df_flattened = raw_df.flatten(raw_df['"RAW_JSON"']['data'])
    df_exploded = df_flattened.select(col("VALUE").alias("record"))
    exprs = [col("record")[i].alias(column_names[i]) for i in range(len(column_names))]
    parsed_df = df_exploded.select(*exprs)

    # --- DEBUG LINE ADDED ---
    # This saves the parsed DataFrame to a temporary table for inspection.
    parsed_df.write.mode("overwrite").save_as_table("TEMP_DEBUG_COLUMNS")

    final_column_mapping = {
        "VIN_1_10": ("VIN", StringType()),
        "CITY": ("City", StringType()),
        "STATE": ("State", StringType()),
        "MAKE": ("Make", StringType()),
        "MODEL": ("Model", StringType()),
        "MODEL_YEAR": ("ModelYear", IntegerType()),
        "EV_TYPE": ("EV_Type", StringType()),
        "ELECTRIC_RANGE": ("ElectricRange", IntegerType()),
        "BASE_MSRP": ("BaseMSRP", IntegerType())
    }

    final_select_exprs = []
    # Convert column_name to uppercase for a case-insensitive lookup.
    for column_name in parsed_df.columns:
        # Standardize the column name to uppercase for the dictionary lookup
        column_name_upper = column_name.upper() 
        if column_name_upper in final_column_mapping:
            # If it's a known column, apply the alias and type cast
            alias, new_type = final_column_mapping[column_name_upper]
            # Use the original column_name to select from the DataFrame
            final_select_exprs.append(col(f'"{column_name}"').cast(new_type).alias(alias))
        elif column_name in column_names:
            # If it's a new/unknown column from the source, pass it through as-is
            final_select_exprs.append(col(f'"{column_name}"'))

    final_df = parsed_df.select(*final_select_exprs)

    # Now, final_df will correctly have columns named "VIN", "City", etc.
    dq_results = run_dq_checks(final_df)

    if dq_results["null_vin_count"] > 0:
        raise ValueError(f"Critical DQ Check Failed: {dq_results['null_vin_count']} Null VINs found. Halting pipeline.")
    
    if dq_results["zero_msrp_count"] > 0:
        print(f"DQ Warning: Found {dq_results['zero_msrp_count']} records with a Base MSRP of 0.")

    final_df.write.mode("overwrite").save_as_table("clean_ev_data_snowpark")
    
    return "Transformation complete. Schema detected dynamically. Data successfully saved."
