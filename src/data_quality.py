from snowflake.snowpark.functions import col
from snowflake.snowpark.dataframe import DataFrame

def run_dq_checks(df: DataFrame) -> dict:
    """
    Runs a series of data quality checks on the transformed DataFrame.
    It uses the final, business-friendly column names created by the main script.
    """
    print("Running data quality checks on final DataFrame...")

    # Check 1: Count of null VINs (should be zero for a primary identifier)
    # The final column alias from the main script is "VIN".
    null_vin_count = df.where(col("VIN").isNull()).count()

    # Check 2: Count of records with a Base MSRP of 0
    # This check flags the known source data quality issue.
    # The final column alias is "BaseMSRP".
    zero_msrp_count = df.where(col("BaseMSRP") == 0).count()

    # Check 3: Count of records with an invalid Model Year (e.g., a future year)
    # The final column alias is "ModelYear".
    invalid_year_count = df.where(col("ModelYear") > 2025).count()
    
    results = {
        "null_vin_count": null_vin_count,
        "zero_msrp_count": zero_msrp_count,
        "invalid_year_count": invalid_year_count
    }
    
    print(f"DQ Check Results: {results}")
    return results
