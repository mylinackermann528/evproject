from snowflake.snowpark.functions import col
from snowflake.snowpark.dataframe import DataFrame

def run_dq_checks(df: DataFrame) -> dict:
    """
    Runs a series of data quality checks on the cleaned and normalized DataFrame.
    It verifies column existence before running each check to prevent runtime errors.
    """
    print("Running data quality checks on final cleaned DataFrame...")

    results = {}

    # Get normalized list of available columns (case-insensitive comparison)
    available_columns = [c.upper() for c in df.columns]

    # Check 1: Count of null VINs
    if "VIN" in available_columns:
        results["null_vin_count"] = df.where(col("VIN").isNull()).count()
    else:
        print("DQ Warning: 'VIN' column not found, skipping null VIN check.")
        results["null_vin_count"] = -1

    # Check 2: Count of records with Base MSRP = 0
    if "BASEMSRP" in available_columns:
        results["zero_msrp_count"] = df.where(col("BaseMSRP") == 0).count()
    else:
        print("DQ Warning: 'BaseMSRP' column not found, skipping zero MSRP check.")
        results["zero_msrp_count"] = -1

    # Check 3: Count of invalid Model Years (future years > 2025)
    if "MODELYEAR" in available_columns:
        results["invalid_year_count"] = df.where(col("ModelYear") > 2025).count()
    else:
        print("DQ Warning: 'ModelYear' column not found, skipping invalid year check.")
        results["invalid_year_count"] = -1

    print(f"DQ Check Results: {results}")
    return results
