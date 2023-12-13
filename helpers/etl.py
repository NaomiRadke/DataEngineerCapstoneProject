import os
# from smart_open import open
import configparser
from datetime import timedelta, datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, year, month, dayofmonth, weekofyear, dayofweek, date_format, avg as _avg, sum as _sum, round as _round, create_map, lit
from pyspark.sql.types import StructField, StructType, IntegerType, DoubleType, StringType, LongType



# Get AWS credentials from .cfg file and save them as environment variables
config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']
aws_access_key = config['AWS']['AWS_ACCESS_KEY_ID']
aws_access_secret_key = config['AWS']['AWS_SECRET_ACCESS_KEY']



def initiate_spark_session():
    """

    Creates a Spark Session

    """
    
    spark  = SparkSession \
    .builder \
    .config("spark.jars.repositories", "https://repos.spark-packages.org/")\
    .config("spark.jars.packages", "saurfang:spark-sas7bdat:2.0.0-s_2.11,org.apache.hadoop:hadoop-aws:3.3.6")\
    .enableHiveSupport() \
    .getOrCreate()
    
    return spark

# Load data
def load_data_from_source(spark, in_path, in_format, columns, row_limit, **options):
    """
    Loads data from the defined input path  with spark into a spark dataframe
  
    Args:
        spark (SparkSession): the spark session
        in_path (str): input path
        in_format (str): format of data source as string,
        columns (list): list of columns to read
        row_limit (int): number of rows that should be loaded, if None, all rows are loaded

    Returns:
        DataFrame: Spark Dataframe
    """
    print("load data"+in_path)
    if row_limit is None:
        df = spark.read.load(in_path, format=in_format, **options).select(columns)
    else:
        df = spark.read.load(in_path, format=in_format, **options).select(columns).limit(row_limit)
        
    return df

def save_to_s3(df, out_path, mode="overwrite", out_format="parquet", partitionBy=None):
    """
    Saves the data frame as parque file to a destination folder on an S3 bucket.

    Args:
        df (DataFrame): Spark Dataframe
        out_path (str): path for saving data frame
        mode (str, optional): Behaviour of save operation when file already exists. Defaults to "overwrite".
        out_format (str, optional): file format in which to save the dataframe. Defaults to "parquet".
        partitionBy (list, optional): list of names of partitioning columns for parquet files. Defaults to None.
    """
    df.write.save(out_path, mode=mode, format=out_format, partitionBy=partitionBy)
    
def cast_type(df, cols):
    """
    Converts the type of each column of the given dataframe into the type indicated in the columns dictionary
    Args:
        df (SparkDataFrame): dataframe to be processed
        cols (dict): column name and type to be converted to like so {"column_name": type}
    """
    for k,v in cols.items():
        if k in df.columns:
            df = df.withColumn(k, df[k].cast(v))
    return df

def convert_sas_date(df, cols):
    """
    Convert SAS date to a YYYY-MM-DD date format

    Args:
        df (SparkDataFrame): dataframe that holds the date columns to be converted
        cols (list): list of date columns to be converted
    """
    for c in [c for c in cols if c in df.columns]:
        df = df.withColumn(c, convert_sas_udf(df[c]))
    return df

def time_delta(date1, date2):
    """
    Calculates the time difference in days between

    Args:
        date1 (str): first date
        date2 (str): second date
    """
    
    if date2 is None:
        return None
    else:
        a = datetime.strptime(date1, date_format)
        b = datetime.strptime(date2, date_format)
        delta = b - a
        return delta.days
    
def load_sas_labels(in_path):
    """
    Reads the SAS labe file and returns a dictionary

    Args:
        in_path (str): path, including name of the file to be read

    Returns:
        dict: dictionary of key value pairs
    """
    with open(in_path) as f:
        f_content = f.read()
        f_content = f_content.replace('\t', '')
        dic = code_mapper(f_content, "i94cntyl")
    return dic    
        
def code_mapper(file, idx):
    """
    extracts the indicated SAS label group and returns the key value pairs as dictionary

    Args:
        file (): content of SAS label file
        idx (str): name of the SAS labels to extract

    Returns:
        dict: a dictionary of key value pairs
    """
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
    in_path, 
    out_path,
    date_out_path,
    in_format="parquet",
    columns=['cicid', 'i94yr', 'i94mon', 'i94res', 'i94mode', 'i94addr', 'i94cit', 'i94bir', 'i94visa', 'arrdate', 'depdate', 'biryear', 'dtaddto', 'gender', 'airline', 'admnum', 'fltno', 'visatype'],
    ):
    """
    Loads immigration data from S3, transforms data and saves the resulting dataframe in S3

    Args:
        spark (SparkSession): the current spark session
        in_path (str): input file location
        out_path (str): desired output file location
        date_out_path (str): desired date output file location 
        in_format (str, optional): file format of input file. Defaults to "parquet".
        columns (list, optional): List of columns to be read. Defaults to ['cicid', 'i94yr', 'i94mon', 'i94res', 'i94mode', 'i94addr', 'i94cit', 'i94bir', 'i94visa', 'arrdate', 'depdate', 'biryear', 'dtaddto', 'gender', 'airline', 'admnum', 'fltno', 'visatype'].

    """
    print("state immigration etl")
    # load data
    immigration = load_data_from_source(spark, in_path=in_path, in_format=in_format, columns=columns, row_limit=None)
    
    # turn SAS date columns to YYYY-MM-DD format
    date_cols = ['arrdate', 'depdate']
    immigration = convert_sas_date(immigration, date_cols)
    
    # add new column for stay duration
    immigration = immigration.withColumn('durationStay', time_delta_udf(immigration.arrdate, immigration.depdate))
    
    # turn numeric columns to integer
    int_cols = ['cicid', 'i94yr', 'i94mon', 'i94res', 'i94mode', 'i94cit', 'i94bir', 'i94visa','biryear', 'fltno', 'durationStay', 'admnum']
    immigration = cast_type(immigration, dict(zip(int_cols, len(int_cols)*[IntegerType()])))
    
    
    # create a new dataframe arrival_date that will be used in the data model
    arrival_date = immigration.select('arrdate').distinct()
    arrival_date = arrival_date.withColumn("day", dayofmonth(arrival_date.arrdate))
    arrival_date = arrival_date.withColumn("month", month(arrival_date.arrdate))
    arrival_date = arrival_date.withColumn("year", year(arrival_date.arrdate))
    arrival_date = arrival_date.withColumn("weekofyear", weekofyear(arrival_date.arrdate))
    arrival_date = arrival_date.withColumn("dayofweek", dayofweek(arrival_date.arrdate))
    
    # save immigration and arrival_date dataframe to S3 in parquet format
    save_to_s3(df=immigration, out_path=out_path, partitionBy=['i94yr', 'i94mon'])
    save_to_s3(df=arrival_date, out_path=date_out_path)
    print("end immigration etl")
    return immigration
    

# ETL demographic data
def process_demographics_data(
    spark, 
    in_path, 
    out_path,
    in_format="csv",
    columns='*',
    header=True,
    sep=";",
    row_limit=None
):
    """_summary_

    Args:
        spark (SparkSession): the current spark session
        in_path (str, optional): input file location. Defaults to "data/us-cities-demographics.csv".
        in_format (str, optional): file format of input file. Defaults to "csv".
        columns (list, optional): List of columns to be read. Defaults to [].
        out_path (str, optional): desired output file location.

    """
    print("START____________________START")
    print("load demographics etl")
    demographics = load_data_from_source(
        spark,
        in_path=in_path,
        in_format=in_format,
        columns=columns,
        header=header,
        sep=sep,
        row_limit=row_limit
        )
    
    # Drop NULL values
    
    # Turn numeric columns into their proper types: Integer or Double
    int8_cols = ['Count', 'Male Population', 'Female Population', 'Total Population', 'Number of Veterans', 'Foreign-born']
    float_cols = ['Median Age', 'Average Household Size']
    demographics = cast_type(demographics, dict(zip(int8_cols, len(int8_cols)*[LongType()])))
    demographics = cast_type(demographics, dict(zip(float_cols, len(float_cols)*[DoubleType()])))
    
    # Aggregate columns per state. This requires first to aggregate by city and to pivot the race+count column
    race_cols = ["Black or African-American","American Indian and Alaska Native", "Hispanic or Latino", "Asian", "White"]

    
    # First, pivot the Race and Count column by City
    race_by_city = demographics.groupBy(["City", "State", "State Code"]).pivot("Race", race_cols).sum("Count")
    
    
    # Then, sum the number of each race per state
    race_by_state = race_by_city.groupBy(["State", "State Code"]).sum()
    race_by_state = race_by_city.groupBy(["State", "State Code"]) \
        .agg(_sum("Black or African-American").alias("blackOrAfricanAmerican"), \
            _sum("American Indian and Alaska Native").alias("americanIndianAndAlaskaNative"), \
            _sum("Hispanic or Latino").alias("hispanicOrLatino"), \
            _sum("Asian").alias("asian"),\
            _sum("White").alias("white"))
    
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
    
    # Rename state and state code column
    demographics = demographics.withColumnRenamed("State", "state")
    demographics = demographics.withColumnRenamed("State Code", "stateCode")
    
    # Turn numeric columns into their proper types: Integer or Double
    int8_cols = ['malePopulation', 'femalePopulation', 'totalPopulation', 'numberOfVeterans', 'foreignBorn', 'blackOrAfricanAmerican', 'americanIndianAndAlaskaNative', 'hispanicOrLatino', 'asian', 'white']
    float_cols = ['medianAge', 'averageHouseholdSize']
    demographics = cast_type(demographics, dict(zip(int8_cols, len(int8_cols)*[LongType()])))
    demographics = cast_type(demographics, dict(zip(float_cols, len(float_cols)*[DoubleType()])))
    
    
    # Save the dataframe as parquet on S3
    save_to_s3(df=demographics, out_path=out_path)
    print("END_______________________END")
    print("end demographis etl")
    return demographics

# Countries 

def process_countries_data(
    spark,
    in_path,
    out_path
    ):
    """_summary_

    Args:
        spark (SparkSession): current Spark session
        in_path (str): _description_
        out_path (str): _description_

    """
    print("START______________________________START")
    print("starting countries etl")
    # Create a country code lookup table to match country code and country name
    countries_dict= load_sas_labels(in_path)
    schema = StructType([StructField('countryCode', StringType(), True),StructField('countryName', StringType(), True) ])
    data_tuples = [(key, value) for key, value in countries_dict.items()]
    countries_df = spark.createDataFrame(data_tuples, schema)
    countries_df = countries_df.withColumn("countryCode", countries_df["countryCode"].cast(IntegerType()))
    
    # save on S3
    save_to_s3(df=countries_df,out_path=out_path)
    
    print("END______________________________________END")
    print("end countries etl")
    return countries_df

## for local debugging
# in_path = "./data/us-cities-demographics.csv"
# in_path = "./data/s3_out_tables/immigration"
# in_format = "parquet"
# out_path = "./data/demographics.parquet"
# columns = "*"

def main ():
    spark = initiate_spark_session()
    bucket = "s3a://udacity-data-engineer/"
    immigration = process_immigration_data(spark, in_path=bucket+"sas_data/", out_path=bucket+"tables/immigration.parquet", date_out_path=bucket+"tables/arrivaldates.parquet")
    demographics = process_demographics_data(spark, in_path=bucket+"us-cities-demographics.csv", out_path=bucket+"tables/demographics.parquet")
    countries = process_countries_data(spark, in_path=bucket+"I94_SAS_Labels_Descriptions.SAS", out_path=bucket+"tables/countries.parquet")
    
    spark.stop()

if __name__ == "__main__" :
    main()
    
    