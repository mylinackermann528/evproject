import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

from transformations import clean_ev_data

S3_RAW_PATH = "s3://ev-project-mylinackermann/bronze/ev_data_raw/"
S3_SILVER_PATH = "s3://ev-project-mylinackermann/curated/ev_data_clean/"

RDS_HOST = "ev-population-db.c0nsiackq1aq.us-east-1.rds.amazonaws.com" 
RDS_PORT = "5432"
RDS_DB = "ev-population-db"
RDS_USER = "evprojectuser"
RDS_PASSWORD = "NewMan2025!!!"
RDS_TABLE = "ev_population_master"

def main():
    args = getResolvedOptions(sys.argv, ['JOB_NAME'])
    sc = SparkContext()
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session
    job = Job(glueContext)
    job.init(args['JOB_NAME'], args)

    try:
        print("STEP 1: Reading raw data...")
        raw_df = spark.read.format("json").option("multiline", "true").load(S3_RAW_PATH)
        print("STEP 1: Successfully read raw data.")
    except Exception as e:
        print("FATAL ERROR IN STEP 1: Could not read source data from S3.")
        raise e

    try:
        print("STEP 2: Transforming data...")
        clean_df = clean_ev_data(raw_df)
        print("STEP 2: Successfully created transformation plan.")
    except Exception as e:
        print("FATAL ERROR IN STEP 2: The 'clean_ev_data' function failed.")
        raise e

    try:
        print("STEP 3: Materializing data and getting count...")
        record_count = clean_df.count()
        print(f"STEP 3: Successfully counted {record_count} records.")
    except Exception as e:
        print("FATAL ERROR IN STEP 3: Could not count records. This often indicates a data corruption or schema mismatch issue.")
        raise e
        
    try:
        print("STEP 4: Writing to S3 Delta Lake...")
        clean_df.write.format("delta").mode("overwrite").save(S3_SILVER_PATH)
        print("STEP 4: Successfully wrote to S3.")
    except Exception as e:
        print("FATAL ERROR IN STEP 4: Could not write to S3 Delta Lake.")
        raise e

    try:
        print("STEP 5: Writing to RDS...")
        jdbc_url = f"jdbc:postgresql://{RDS_HOST}:{RDS_PORT}/{RDS_DB}"
        clean_df.write.format("jdbc") \
            .option("url", jdbc_url) \
            .option("dbtable", RDS_TABLE) \
            .option("user", RDS_USER) \
            .option("password", RDS_PASSWORD) \
            .option("driver", "org.postgresql.Driver") \
            .mode("overwrite") \
            .save()
        print("STEP 5: Successfully wrote to RDS.")
    except Exception as e:
        print("FATAL ERROR IN STEP 5: Could not write to RDS.")
        raise e

    job.commit()

if __name__ == "__main__":
    main()
