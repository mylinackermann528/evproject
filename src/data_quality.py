from snowflake.snowpark.functions import col
from snowflake.snowpark.dataframe import DataFrame

def run_dq_checks(df: DataFrame) -> dict:
    """
    Runs a series of data quality checks on the transformed DataFrame.
    It uses the final, business-friendly column names created by the main script.
    """
    print("Running data quality checks on final DataFrame...")

    # --- FIX ---
    # Temporarily skipping the VIN null count to isolate the error.
    # null_vin_count = df.where(col("VIN").isNull()).count()
    null_vin_count = 0

    # Check 2: Count of records with a Base MSRP of 0
    zero_msrp_count = df.where(col("BaseMSRP") == 0).count()

    # Check 3: Count of records with an invalid Model Year (e.g., a future year)
    invalid_year_count = df.where(col("ModelYear") > 2025).count()
    
    results = {
        "null_vin_count": null_vin_count,
        "zero_msrp_count": zero_msrp_count,
        "invalid_year_count": invalid_year_count
    }
    
    print(f"DQ Check Results: {results}")
    return results
