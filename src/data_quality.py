from snowflake.snowpark.functions import col
from snowflake.snowpark.dataframe import DataFrame

def run_dq_checks(df: DataFrame) -> dict:
    """
    Runs a series of data quality checks on the transformed DataFrame.
    It checks for column existence before running a check to avoid errors.
    """
    print("Running data quality checks on final DataFrame...")

    # --- FIX: Made checks conditional to prevent errors ---

    results = {}
    
    # Get a list of available columns in the final DataFrame, standardized to uppercase
    available_columns = [c.upper() for c in df.columns]

    # Check 1: Count of null VINs
    if "VIN" in available_columns:
        results["null_vin_count"] = df.where(col("VIN_1_10").isNull()).count()
    else:
        print("DQ Warning: 'VIN' column not found, skipping null count check.")
        results["null_vin_count"] = -1 # Use -1 to indicate the check was skipped

    # Check 2: Count of records with a Base MSRP of 0
    if "BASEMSRP" in available_columns:
        results["zero_msrp_count"] = df.where(col("BASE_MSRP") == 0).count()
    else:
        print("DQ Warning: 'BaseMSRP' column not found, skipping zero MSRP check.")
        results["zero_msrp_count"] = -1

    # Check 3: Count of records with an invalid Model Year
    if "MODELYEAR" in available_columns:
        results["invalid_year_count"] = df.where(col("MODEL_YEAR") > 2025).count()
    else:
        print("DQ Warning: 'ModelYear' column not found, skipping invalid year check.")
        results["invalid_year_count"] = -1
    
    print(f"DQ Check Results: {results}")
    return results
