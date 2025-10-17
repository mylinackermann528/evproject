import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import lit

from transformations import clean_ev_data

S3_RAW_PATH = "s3://ev-project-mylinackermann/bronze/ev_data_raw/"
S3_SILVER_PATH = "s3://ev-project-mylinackermann/curated/ev_data_clean/"
RDS_HOST = "<YOUR_RDS_POSTGRES_ENDPOINT_WITHOUT_PORT>" 
RDS_PORT = "5432"
RDS_DB = "<YOUR_DB_NAME>"
RDS_USER = "evprojectuser"
RDS_PASSWORD = "NewMan2025!!!"
RDS_TABLE = "ev_population_master"
GLUE_CONNECTION_NAME = "Aurora Connection"

COLUMN_NAMES = [
    "VIN_1_10", "County", "City", "State", "Postal_Code", "Model_Year", 
    "Make", "Model", "Electric_Vehicle_Type", "CAFV_Eligibility", 
    "Electric_Range", "Base_MSRP", "Legislative_District", "DOL_Vehicle_ID", 
    "Vehicle_ID", "2020_Census_Tract"
]

def main():
    args = getResolvedOptions(sys.argv, ['JOB_NAME'])
    sc = SparkContext()
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session
    job = Job(glueContext)
    job.init(args['JOB_NAME'], args)

    print(f"Reading raw data from: {S3_RAW_PATH}")
    
    raw_df = spark.read.format("json").option("multiline", "true").load(S3_RAW_PATH)
    
    if raw_df.isEmpty():
        raise Exception(f"Input file is empty or path is incorrect: {S3_RAW_PATH}")

    print("Starting data transformation and cleaning...")
    clean_df = clean_ev_data(raw_df, COLUMN_NAMES)
    
    print(f"Transformation complete. Writing {clean_df.count()} records.")
    
    print(f"Writing master data to S3 Delta Lake: {S3_SILVER_PATH}")
    
    clean_df.write.format("delta").mode("overwrite").save(S3_SILVER_PATH)

    print(f"Writing master data to RDS table: {RDS_TABLE}")
    
    jdbc_url = f"jdbc:postgresql://{RDS_HOST}:{RDS_PORT}/{RDS_DB}"
    
    try:
        clean_df.write.format("jdbc") \
            .option("url", jdbc_url) \
            .option("dbtable", RDS_TABLE) \
            .option("user", RDS_USER) \
            .option("password", RDS_PASSWORD) \
            .option("driver", "org.postgresql.Driver") \
            .option("connectionName", GLUE_CONNECTION_NAME) \
            .mode("overwrite") \
            .save()
        print(f"Successfully wrote data to RDS using connection: {GLUE_CONNECTION_NAME}")
    except Exception as e:
        print(f"WARNING: Failed to write to RDS. Check VPC, Security Group, and JDBC details. Error: {e}")

    job.commit()

if __name__ == "__main__":
    main()
