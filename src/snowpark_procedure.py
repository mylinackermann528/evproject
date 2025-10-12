from snowflake.snowpark import Session
from snowflake.snowpark.functions import col
from snowflake.snowpark.types import StringType, IntegerType

def main(session: Session):
    """
    Main transformation function to be executed as a Snowflake Stored Procedure.
    The session object is passed in automatically by the Snowflake environment.
    """
    print("Starting server-side EV data transformation with Snowpark...")

    # 1. Load the raw data from the VARIANT table
    # The procedure runs in the context of the database/schema it was called from.
    raw_df = session.table("raw_ev_data")

    # 2. Define the column names based on the JSON structure
    column_names = [
        "vin_10", "county", "city", "state", "postal_code", "model_year", 
        "make", "model", "ev_type", "cafv_eligibility", "electric_range", 
        "base_msrp", "legislative_district", "dol_vehicle_id", "vehicle_location", 
        "electric_utility", "census_tract_2020"
    ]
    
    # 3. Parse the nested JSON structure using the flatten method
    # This is the most robust way to un-nest arrays in Snowpark.
    # The 'VALUE' column created by flatten contains each record from the original array.
    # Correctly reference the uppercase column name "RAW_JSON"
    df_flattened = raw_df.flatten(col('"RAW_JSON":data'))
    df_exploded = df_flattened.select(col("VALUE").alias("record"))
    
    # Dynamically select and alias columns
    exprs = [col("record")[i].alias(column_names[i]) for i in range(len(column_names))]
    parsed_df = df_exploded.select(*exprs)

    # 4. Select final columns, cast data types, and apply friendly aliases
    final_df = parsed_df.select(
        col("vin_10").cast(StringType()).alias("VIN"),
        col("city").cast(StringType()).alias("City"),
        col("state").cast(StringType()).alias("State"),
        col("make").cast(StringType()).alias("Make"),
        col("model").cast(StringType()).alias("Model"),
        col("model_year").cast(IntegerType()).alias("ModelYear"),
        col("ev_type").cast(StringType()).alias("EV_Type"),
        col("electric_range").cast(IntegerType()).alias("ElectricRange"),
        col("base_msrp").cast(IntegerType()).alias("BaseMSRP")
    )
    
    # 5. Save the transformed data to a final table in Snowflake
    final_df.write.mode("overwrite").save_as_table("clean_ev_data_snowpark")
    
    print("Transformation complete. Data saved to 'clean_ev_data_snowpark' table.")
    
    # The return value of the function is the return value of the Stored Procedure
    return "Successfully processed and saved data."
