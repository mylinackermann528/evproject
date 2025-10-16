from snowflake.snowpark import Session
from snowflake.snowpark.functions import col, flatten
from snowflake.snowpark.types import StringType, IntegerType

from data_quality import run_dq_checks

def main(session: Session):
    """
    Main transformation function for the Snowpark Stored Procedure.
    It orchestrates the data loading, transformation, and quality checks.
    """
    print("Starting server-side EV data transformation with Snowpark...")

    # 1. Load the raw data from the VARIANT table
    raw_df = session.table("raw_ev_data")

    # 2. Dynamically discover the schema from the JSON metadata
    print("Discovering schema from JSON metadata...")
    column_metadata_df = raw_df.select(flatten(raw_df['"RAW_JSON"']['meta']['view']['columns']))
    column_names_rows = column_metadata_df.select(col("VALUE")['fieldName'].alias("name")).collect()
    column_names = [row['NAME'] for row in column_names_rows]
    print(f"Discovered {len(column_names)} columns.")

    # 3. Parse the nested JSON data array
    df_flattened = raw_df.flatten(raw_df['"RAW_JSON"']['data'])
    df_exploded = df_flattened.select(col("VALUE").alias("record"))
    exprs = [col("record")[i].alias(column_names[i]) for i in range(len(column_names))]
    parsed_df = df_exploded.select(*exprs)

    # 4. Define the mapping for our core, business-friendly columns
    # --- THIS IS THE FIX ---
    # The dictionary keys are now UPPERCASE to match the DataFrame's column names.
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

    # 5. Build the final select expression list
    final_select_exprs = []
    # parsed_df.columns contains uppercase strings (e.g., 'SID', 'VIN_1_10')
    for column_name in parsed_df.columns:
        if column_name in final_column_mapping:
            # If it's a known column, apply the alias and type cast
            alias, new_type = final_column_mapping[column_name]
            final_select_exprs.append(col(column_name).cast(new_type).alias(alias))
        elif column_name in column_names:
            # If it's a new/unknown column from the source, pass it through as-is
            final_select_exprs.append(col(column_name))

    final_df = parsed_df.select(*final_select_exprs)

    # 6. Run the imported Data Quality checks
    # Now, final_df will correctly have columns named "VIN", "City", etc.
    dq_results = run_dq_checks(final_df)

    # 7. Validate DQ results and handle failures or warnings
    if dq_results["null_vin_count"] > 0:
        raise ValueError(f"Critical DQ Check Failed: {dq_results['null_vin_count']} Null VINs found. Halting pipeline.")
    
    if dq_results["zero_msrp_count"] > 0:
        print(f"DQ Warning: Found {dq_results['zero_msrp_count']} records with a Base MSRP of 0.")

    # 8. Save the transformed data to the final table
    final_df.write.mode("overwrite").save_as_table("clean_ev_data_snowpark")
    
    return "Transformation complete. Schema detected dynamically. Data successfully saved."

