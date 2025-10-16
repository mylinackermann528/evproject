from snowflake.snowpark import Session
from snowflake.snowpark.functions import col, flatten, regexp_replace
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

    # Normalize all column names to uppercase
    parsed_df = parsed_df.select(*[col(c).alias(c.upper()) for c in parsed_df.columns])

    # Mapping of uppercase source columns to final clean names and types
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
    # Loop through all columns of the parsed DataFrame
    for column_name in parsed_df.columns:
        # Check if the column is in our mapping dictionary
        if column_name in final_column_mapping:
            alias, new_type = final_column_mapping[column_name]
            # Remove extra quotes and cast to the correct type
            clean_col = regexp_replace(col(column_name), r'^"|"$', '')
            final_select_exprs.append(clean_col.cast(new_type).alias(alias))
        else:
            # If the column is not in the mapping, keep it as is
            final_select_exprs.append(col(column_name))
            
    final_df = parsed_df.select(*final_select_exprs)

    # Reorder columns for a cleaner final table
    desired_order = ["VIN", "Make", "Model", "ModelYear", "EV_Type", "ElectricRange", "BaseMSRP", "City", "State"]
    
    # Get a list of columns that are actually in the final DataFrame
    final_ordered_cols = [c for c in desired_order if c in final_df.columns]
    
    # Get the remaining columns to append them to the end, ensuring no data is lost
    remaining_cols = [c for c in final_df.columns if c not in final_ordered_cols]
    
    final_df = final_df.select(*final_ordered_cols, *remaining_cols)

    dq_results = run_dq_checks(final_df)

    if dq_results["null_vin_count"] > 0:
        raise ValueError(f"Critical DQ Check Failed: {dq_results['null_vin_count']} Null VINs found. Halting pipeline.")

    if dq_results["zero_msrp_count"] > 0:
        print(f"DQ Warning: Found {dq_results['zero_msrp_count']} records with a Base MSRP of 0.")

    final_df.write.mode("overwrite").save_as_table("clean_ev_data_snowpark")

    return "Transformation complete. Schema detected dynamically. Data successfully cleaned and saved."
