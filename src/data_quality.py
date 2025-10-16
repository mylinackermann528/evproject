from snowflake.snowpark.functions import col, count, lit

def run_dq_checks(df):
    print("Running data quality checks...")

    total_rows = df.count()

    null_vin_count = df.where(col("VIN").isNull()).count()

    duplicate_vin_count = df.groupBy("VIN").agg(count("*").alias("c")).where(col("c") > 1).count()

    invalid_year_count = df.where((col("ModelYear") < 1990) | (col("ModelYear") > 2025)).count()
    
    valid_ev_types = ["Battery Electric Vehicle (BEV)", "Plug-in Hybrid Electric Vehicle (PHEV)"]
    invalid_ev_type_count = df.where(~col("EV_Type").in_(valid_ev_types)).count()

    zero_msrp_count = df.where(col("BaseMSRP") == 0).count()

    dq_results = {
        "total_rows": total_rows,
        "null_vin_count": null_vin_count,
        "duplicate_vin_count": duplicate_vin_count,
        "invalid_year_count": invalid_year_count,
        "invalid_ev_type_count": invalid_ev_type_count,
        "zero_msrp_count": zero_msrp_count
    }

    print(f"Data quality check results: {dq_results}")
    return dq_results

