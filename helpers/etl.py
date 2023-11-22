import os, re
import configparser
from datetime import timedelta, datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, when, lower, isnull, year, month, dayofmonth, weekofyear, dayofweek, date_format, avg as _avg, sum as _sum, round as _round, create_map, lit
from pyspark.sql.types import StructField, StructType, IntegerType, DoubleType, StringType



# Save AWS credentials as environment variables


def initiate_spark_session():
    
    spark  = SparkSession \
    .builder \
    .appName("Data Engineering Project") \
    .getOrCreate()
    
    return spark

# Load data
def load_data_from_source(spark, in_path, in_format, columns, row_limit, **options):
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
        df = spark.read.load(in_path, format=in_format, **options).select(columns)
    else:
        df = spark.read.load(in_path, format=in_format, **options).select(columns).limit(row_limit)
        
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

def time_delta(date1, date2):
    """
    Calculates the time difference in days between

    Args:
        date1 (_type_): _description_
        date2 (_type_): _description_
    """
    
    if date2 is None:
        return None
    else:
        a = datetime.strptime(date1, date_format)
        b = datetime.strptime(date2, date_format)
        delta = b - a
        return delta.days
    
def load_sas_labels():
    with open('./data/I94_SAS_Labels_Descriptions.SAS') as f:
        f_content = f.read()
        f_content = f_content.replace('\t', '')
        dic = code_mapper(f_content, "i94cntyl")
    return dic    
        
def code_mapper(file, idx):
    f_content2 = file[file.index(idx):]
    f_content2 = f_content2[:f_content2.index(';')].split('\n')
    f_content2 = [i.replace("'", "") for i in f_content2]
    dic = [i.split('=') for i in f_content2[1:]]
    dic = dict([i[0].strip(), i[1].strip()] for i in dic if len(i) == 2)
    return dic
    

# user-defined function to turn SAS dates into YYYY-MM-DD format
date_format="%Y-%m-%d"
convert_sas_udf = udf(lambda x: x if x is None else (timedelta(days=x) + datetime(1960, 1, 1)).strftime(date_format))   
time_delta_udf = udf(time_delta) 

    
# ETL immigration data
def process_immigration_data(
    spark, 
    in_path="data/sas_data", 
    in_format="parquet",
    columns=['cicid', 'i94yr', 'i94mon', 'i94res', 'i94mode', 'i94addr', 'i94cit', 'i94bir', 'i94visa', 'arrdate', 'depdate', 'biryear', 'dtaddto', 'gender', 'airline', 'admnum', 'fltno', 'visatype'],
    out_path="s3a://data-engineer-capstone/immigration.parquet",
    date_out_path="s3a://data-engineer-capstone/date.parquet"):
    """
    - loads data
    - transforms data
    - saves data in S3
    """
    # load data
    immigration = load_data_from_source(spark, in_path=in_path, in_format=in_format, columns=columns, row_limit=None)
    
    # turn numeric columns to either integer or double
    int_cols = ['cicid', 'i94yr', 'i94mon', 'i94res', 'i94mode', 'i94cit', 'i94bir', 'i94visa', 'arrdate', 'depdate', 'biryear']
    immigration = cast_type(immigration, dict(zip(int_cols, len(int_cols)*[IntegerType()])))
    
    # turn SAS date columns to YYYY-MM-DD format
    date_cols = ['arrdate', 'depdate']
    immigration = convert_sas_date(immigration, date_cols)
    
    # add new column for stay duration
    immigration = immigration.withColumn('stay_duration', time_delta_udf(immigration.arrdate, immigration.depdate))
    
    # create a new dataframe arrival_date that will be used in the data model
    arrival_date = immigration.select('arrdate').distinct()
    arrival_date = arrival_date.withColumn("day", dayofmonth(arrival_date.arrdate))
    arrival_date = arrival_date.withColumn("month", month(arrival_date.arrdate))
    arrival_date = arrival_date.withColumn("year", year(arrival_date.arrdate))
    arrival_date = arrival_date.withColumn("weekofyear", weekofyear(arrival_date.arrdate))
    arrival_date = arrival_date.withColumn("dayofweek", dayofweek(arrival_date.arrdate))
    
    # save immigration and arrival_date dataframe to S3 in parquet format
    save_to_s3(df=immigration, out_path=out_path)
    save_to_s3(df=arrival_date, out_path=date_out_path)
    
    return immigration
    

# ETL demographic data
def process_demographics_data(
    spark, 
    in_path="data/us-cities-demographics.csv", 
    in_format="csv",
    columns='*',
    out_path="s3a://data-engineer-capstone/demographics.parquet",
    header=True,
    sep=";",
    row_limit=None
):
    """_summary_

    Args:
        spark (_type_): _description_
        in_path (str, optional): _description_. Defaults to "data/us-cities-demographics.csv".
        in_format (str, optional): _description_. Defaults to "csv".
        columns (list, optional): _description_. Defaults to [].
        out_path (str, optional): _description_. Defaults to "s3a://data-engineer-capstone/demographics.parquet".

    Returns:
        _type_: _description_
    """
    demographics = load_data_from_source(
        spark,
        in_path=in_path,
        in_format=in_format,
        columns=columns,
        out_path=out_path,
        header=header,
        sep=sep,
        row_limit=row_limit
        )
    
    # Turn numeric columns into their proper types: Integer or Double
    int_cols = ['Count', 'Male Population', 'Female Population', 'Total Population', 'Number of Veterans', 'Foreign-born']
    float_cols = ['Median Age', 'Average Household Size']
    demographics = cast_type(demographics, dict(zip(int_cols, len(int_cols)*[IntegerType()])))
    demographics = cast_type(demographics, dict(zip(float_cols, len(float_cols)*[DoubleType()])))
    
    # Aggregate columns per state. This requires first to aggregate by city and to pivot the race+count column
    race_cols = ["Black or African-American","American Indian and Alaska Native", "Hispanic or Latino", "Asian", "White"]

    
    # First, pivot the Race and Count column by City
    race_by_city = demographics.groupBy(["City", "State", "State Code"]).pivot("Race", race_cols).sum("Count")
    
    
    # Then, sum the number of each race per state
    race_by_state = race_by_city.groupBy(["State", "State Code"]).sum()
    race_by_state = race_by_city.groupBy(["State", "State Code"]) \
        .agg(_sum("Black or African-American").alias("blackOrAfricanAmerican"), \
            _sum("American Indian and Alaska Native").alias("amerianIndianAndAlaskaNative"), \
            _sum("Hispanic or Latino").alias("hispanicOrLatino"), \
            _sum("Asian").alias("Asian"),\
            _sum("White").alias("White"))
    
    # Also sum the non-race columns per state
    df_by_state = demographics.groupBy(["State", "State Code"]) \
        .agg(_sum("Male Population").alias("malePopulation"), \
            _sum("Female Population").alias("femalePopulation"), \
            _sum("Total Population").alias("totalPopulation"), \
            _sum("Number of Veterans").alias("numberOfVeterans"), \
            _sum("Foreign-born").alias("foreignBorn"), \
            _avg("Median Age").alias("medianAge"), \
            _avg("Average Household Size").alias("averageHouseholdSize"))
    
    # Round the medianAge and averageHouseholdSize column to two decimals
    df_by_state = df_by_state.withColumn("medianAge", _round(df_by_state["medianAge"], 2))
    df_by_state = df_by_state.withColumn("averageHouseholdSize", _round(df_by_state["averageHouseholdSize"], 2))
    
    # Join the two dataframes
    demographics = df_by_state.join(other=race_by_state, on=["State", "State Code"], how="inner")
    
    # Save the dataframe as parquet on S3
    save_to_s3(df=demographics, out_path=out_path)
    
    return demographics

# Countries 

def process_countries_data(
    spark,
    in_path,
    in_format,
    out_path,
    columns,
    header=True):
    
    # Create a country code lookup table to match country code and country name
    countries_dict= load_sas_labels()
    schema = StructType([StructField('countryCode', StringType(), True),StructField('countryName', StringType(), True) ])
    data_tuples = [(key, value) for key, value in countries_dict.items()]
    countries_df = spark.createDataFrame(data_tuples, schema)
    
    # save on S3
    
    return countries_df


if __name__ == "__main__" :
    spark = initiate_spark_session()
    immigration = process_immigration_data(spark)
    demographics = process_demographics_data(spark)
    countries = process_countries_data(spark)