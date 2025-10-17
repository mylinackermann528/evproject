from snowflake.snowpark.functions import col
from snowflake.snowpark.dataframe import DataFrame

def run_dq_checks(df: DataFrame) -> dict:
    print("Running data quality checks on final cleaned DataFrame...")

    results = {}

    available_columns = [c.upper() for c in df.columns]

    if "VIN" in available_columns:
        results["null_vin_count"] = df.where(col("VIN").isNull()).count()
    else:
        print("DQ Warning: 'VIN' column not found, skipping null VIN check.")
        results["null_vin_count"] = -1

    if "BASEMSRP" in available_columns:
        results["zero_msrp_count"] = df.where(col("BaseMSRP") == 0).count()
    else:
        print("DQ Warning: 'BaseMSRP' column not found, skipping zero MSRP check.")
        results["zero_msrp_count"] = -1

    if "MODELYEAR" in available_columns:
        results["invalid_year_count"] = df.where(col("ModelYear") > 2025).count()
    else:
        print("DQ Warning: 'ModelYear' column not found, skipping invalid year check.")
        results["invalid_year_count"] = -1

    print(f"DQ Check Results: {results}")
    return results
