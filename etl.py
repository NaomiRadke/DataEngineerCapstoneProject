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
    .appName("Data Engineering Project") \
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

def save_to_s3(df, out_path, mode="overwrite", out_format="parquet"):
    """
    Saves the data frame as parque file to a destination folder on an S3 bucket.
    """
    df.write.save(out_path, mode=mode, format=out_format)
    
def cast_type(df, cols):
    """
    Converts the type of each column of the given dataframe into the type indicated in the columns dictionary
    Args:
        df (:obj:`SparkDataFrame`): dataframe to be processed
        cols (:obj:`dict`): column name and type to be converted to like so {"column_name": type}
    """
    for k,v in cols.items():
        if k in df.columns:
            df = df.withColumn(k, df[k].cast(v))
    return df

def convert_sas_date(df, cols):
    """
    Convert SAS date to a YYYY-MM-DD date format

    Args:
        df (:obj:`SparkDataFrame`): dataframe that holds the date columns to be converted
        cols (:obj:`list`): list of date columns to be converted
    """
    for c in [c for c in cols if c in df.columns]:
        df = df.withColumn(c, convert_sas_udf(df[c]))
    return df

# user-defined function to turn SAS dates into YYYY-MM-DD format
convert_sas_udf = udf(lambda x: x if x is None else (timedelta(days=x) + datetime(1960, 1, 1)).strftime(date_format))    

    
    
    
# ETL immigration data
def etl_immigration(
    spark, 
    in_path="data/sas_data", 
    in_format="parquet",
    columns=['cicid', 'i94yr', 'i94mon', 'i94res', 'i94mode', 'i94addr', 'i94cit', 'i94bir', 'i94visa', 'arrdate', 'depdate', 'biryear', 'dtaddto', 'gender', 'airline', 'admnum', 'fltno', 'visatype'],
    out_path="s3a://data-engineer-capstone/immigration.parquet"):
    """
    - loads data
    - transforms data
    - saves data in S3
    """
    # load data
    immigration = load_data_from_source(spark, in_path=in_path, in_format=in_format, columns=columns, row_limit=10)
    
    # turn numeric columns to either integer or double
    int_cols = ['cicid', 'i94yr', 'i94mon', 'i94res', 'i94mode', 'i94cit', 'i94bir', 'i94visa', 'arrdate', 'depdate', 'biryear']
    immigration = cast_type(immigration, dict(zip(int_cols, len(int_cols)*[IntegerType()])))
    
    # turn SAS date columns to YYYY-MM-DD format
    date_cols = ['arrdate', 'depdate']
    date_format="%Y-%m-%d"
    
    
    # add new column for stay duration
    
    # save to S3 in parquet format

# ETL demographic data

#

if __name__ == "__main__" :
    spark = initiate_spark_session()