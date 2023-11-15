import os, re
import configparser
from datetime import timedelta, datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, when, lower, isnull, year, month, dayofmonth, hour, weekofyear, dayofweek, date_format, to_date
from pyspark.sql.types import StructField, StructType, IntegerType, DoubleType



# Save AWS credentials as environment variables


def initiate_spark_session():
    
    spark  = SparkSession \
    .builder \
    .appName("Capstone Project") \
    .getOrCreate()
    
    return spark

# Load data
def load_data_from_source(spark, in_path, in_format, columns, row_limit):
    """
    Loads data from the defined input path (in_path) with spark into a spark dataframe
    args:
        spark:      the spark session
        in_path:    input path (string)
        in_format:  format of data source as string, e.g. 'json'
        columns:    list of columns to read
        row_limit:  number of rows that should be loaded, if None, all rows are loaded    
    """
    if row_limit is None:
        df = spark.read.load(in_path, format=in_format).select(columns)
    else:
        df = spark.read.load(in_path, format=in_format).select(columns).limit(row_limit)
        
    return df

def save_to_s3():
    """
    Saves the data frame as parque file to a destination folder on an S3 bucket.
    """
# ETL immigration data
def etl_immigration():
    """
    - loads data
    - transforms data
    - saves data in S3
    """
    # load data
    
    # rename columns
    
    # turn numeric columns to either integer or double
    
    # save to S3 in parquet format

# ETL demographic data

#

if __name__ == "__main__" :
    spark = initiate_spark_session()