###  Following is the path for postgres-jar
###  https://repo1.maven.org/maven2/org/postgresql/postgresql/42.2.6/postgresql-42.2.6.jar
###  
###
###
#import findspark
#findspark.init()
#import pyspark
from pyspark import SparkConf
import os
import pandas as pd
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, explode, monotonically_increasing_id
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DoubleType,
    TimestampType,
    DateType,
)
import configparser
config = configparser.ConfigParser()
config.read("cloud.cfg")

os.environ["AWS_ACCESS_KEY_ID"] = config["AWS"]["AWS_ACCESS_KEY_ID"]
os.environ["AWS_SECRET_ACCESS_KEY"] = config["AWS"]["AWS_SECRET_ACCESS_KEY"]

def create_spark_session(key_id,secret_access_key,region):
    """
    Funtion to create a spark session to process data
    """

    jars = [
                "Jars/postgresql-42.2.6.jar"
        ]

    conf = (
            SparkConf()
            .setAppName("S3 to Redshift")
            .set("spark.driver.extraClassPath", ":".join(jars))
            .set("spark.jars.packages","org.apache.hadoop:hadoop-aws:2.7.2")
            .set("spark.hadoop.fs.s3a.access.key", key_id)
            .set("spark.hadoop.fs.s3a.secret.key", secret_access_key)
            .set("spark.hadoop.fs.s3a.path.style.access", True)
            .set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            .set("com.amazonaws.services.s3.enableV4", True)
            .set("spark.hadoop.fs.s3a.endpoint", f"s3-{region}.amazonaws.com")
            .set("spark.executor.extraJavaOptions", "-Dcom.amazonaws.services.s3.enableV4=true")
            .set("spark.driver.extraJavaOptions", "-Dcom.amazonaws.services.s3.enableV4=true")
        )
    spark = SparkSession.builder.master("local").config(conf=conf).getOrCreate()

    return spark

def write_to_redshift(spark, df, table):
    """
    Function to write the code on to Redshift
    """

    print("Writing to {} ...".format(table))

    endpoint_url = config['ENDPOINT']['URL']
    iam_role = config['IAM_ROLE']['ARN']

    s3_opt_path = config['S3_TABLES']['PROCESSED_PARQ_FILES_PATH']

    df.printSchema()
    df.show()

    df.write.mode("append").format("jdbc") \
               .option("url", f"jdbc:postgresql://{endpoint_url}") \
               .option("dbtable", f"{table}") \
               .option("user", config.get('CLUSTER','DB_USER')) \
               .option("password", config.get('CLUSTER','DB_PASSWORD')) \
               .save()
    print("-------------------------------------------------")
    print("Completed Writing to {} table...".format(table))
    print("-------------------------------------------------")


def loading_I94_code_tables(spark, json_file):
    """
    Reading the json data and exploding each and every  I94 columns seperetely
        And load then into redshift
    """
    #Checking with local file
    
    label_desc_df = spark.read.json(json_file)
    print("Exploding Json File....")
    label_desc_df.printSchema()

    # MODES TABLE
    modes_df = (
        label_desc_df.select(explode(col("I94MODE")))
        .toDF("mode")
        .selectExpr("CAST(mode.key AS INT) mode_id", "mode.val as mode_name")
    )

    write_to_redshift(spark, modes_df, "MODES")

    # STATES TABLE
    addr_df = (
        label_desc_df.select(explode(col("I94ADDR")))
        .toDF("addr")
        .selectExpr("addr.key as state_code", "addr.val as state")
    )

    write_to_redshift(spark, addr_df, "STATES")

    # CITIES  TABLE
    city_res_df = (
        label_desc_df.select(explode(col("I94CIT_I94RES")))
        .toDF("city_res")
        .selectExpr("CAST(city_res.key AS INT) city_code", "city_res.val as city")
    )

    write_to_redshift(spark, city_res_df, "CITIES")

    # PORTS  TABLE
    ports_df = (
        label_desc_df.select(explode(col("I94PORT")))
        .toDF("port")
        .selectExpr("port.key as port_code", "port.val as port_name")
    )

    write_to_redshift(spark, ports_df, "PORTS")

    # VISAS TABLE
    visa_code_df = (
        label_desc_df.select(explode(col("I94VISA")))
        .toDF("visa")
        .selectExpr("CAST(visa.key AS INT) visa_id", "visa.val as visa_name")
    )
    write_to_redshift(spark, visa_code_df, "VISAS")

@udf(DateType())
def convert_sas_date(epoch):
    """
    UDF to convert SAS date time into DateType() if given
        else return a default date 
        
    """
    if epoch:
        time = pd.to_timedelta(epoch, unit='D') + pd.datetime(1960, 1, 1)
        return time
    else:
        return datetime(9999,9,9)

def loading_immgration_table(spark, sas_data):
    """
    Funtion to load immigration data
        :param spark       : SparkSession
        :param sas_data    : path to sas parquet files
    """
    immigration_df = spark.read.parquet(sas_data)
    
    ###
    ### Filtering the data with immigration mode of Air
    ###
    immigration_df = immigration_df.filter(col('i94mode')== 1)
    ###
    ### Selecting only the required columns
    ###
    immig_df = immigration_df.selectExpr(["CAST(cicid AS BIGINT) ",
                                            "CAST(i94yr AS INT) ",
                                            "CAST(i94mon AS INT) ",
                                            "CAST(i94cit AS INT)  ",
                                            "CAST(i94res AS INT) ",
                                            "i94port",
                                            "CAST(arrdate AS BIGINT) arrival_date ",
                                            "CAST(i94mode as INT) ",
                                            "i94addr",
                                            "CAST(depdate AS BIGINT) departure_date ",
                                            "CAST(i94visa AS INT)",
                                            "CAST(biryear AS INT) ",
                                            "gender",
                                            "visatype"])

    #immig_df.show(10)
    ###
    ### following .withColumn will overwrite the sas date numberic with dateType()
    ### as the given alias column name is same as the inputcolumn name i.e., arrival_date and departure_date
    ###
    immig_df = immig_df.withColumn("arrival_date",convert_sas_date("arrival_date")).withColumn("departure_date",convert_sas_date("departure_date"))
    col_defaults = {
                 "gender" :'NA',
                 "i94addr":'NO ADDR',
                 "biryear" : -999999
                }

    cleansed_immig_df = immig_df.na.fill(col_defaults)
    write_to_redshift(spark, cleansed_immig_df,"IMMIGRANTS")


def loading_us_demographics_table(spark,file):
    """
    Funtion to load immigration data
        :param spark  : SparkSession
        :param file   : path to us_demographs_csv.csv
    """
    us_cities_df = spark.read.csv(file,sep = ";",header = True)
    
    df = us_cities_df.selectExpr(["City as city",
                                          "State as state",
                                          "CAST(`Median Age` AS DOUBLE) median_age",
                                          "CAST(`Male Population` AS INT) male_population",
                                          "CAST(`Female Population` AS INT) female_population",
                                          "CAST(`Total Population` AS INT) total_population",
                                         "CAST(`Number of Veterans` AS INT) as veterans_count",
                                         "CAST(`Foreign-born` AS INT) foriegn_born",
                                          "Race",
                                          "`State code` AS state_code"
                                         ])
    
    cleansed_us_cities_df = df.na.fill(-999,["veterans_count","foriegn_born"])
    write_to_redshift(spark, cleansed_us_cities_df,"US_CITY_DEMOGRAPHICS")




def loading_airports_table(spark,file):
    """
    Funtion to load immigration data
        :param spark  : SparkSession
        :param file   : path to airport_codes_csv.csv file
    """
    
    airport_codes_df = spark.read.csv(file,header = True)
    ###
    ### AIRPORTS TABLE
    ### airport_codes can be connected by 'local_code' with 'PORT_CODE' column PORTS DIMENSION TABLE
    ### Selecting only By country
    ###


    clean_airport_codes_df = airport_codes_df.filter(col("iso_country")== 'US')

    clean_airport_codes_df = clean_airport_codes_df.selectExpr("ident",
                                                    "type",
                                                    "name",
                                                    "CAST(elevation_ft as INT)",
                                                    "iso_country",
                                                    "iso_region",
                                                    "municipality",
                                                    "gps_code",
                                                    "local_code",
                                                    "coordinates")

    write_to_redshift(spark, clean_airport_codes_df,"AIRPORTS")

def loading_temperatures_table(spark,file):
    """
    Funtion to load immigration data
        :param spark  : SparkSession
        :param file   : path to GlobalLandTemperatures parquet files
    """
    global_tempetature_df = spark.read.parquet(file)
    global_tempetature_df.createOrReplaceTempView("globalTempTable")

    ####
    #### Cleaning the GlobalLandTemperaturesByCity.csv data and selecting only 'United States' records
    ####

    cleansed_global_temp_df = spark.sql("""SELECT TO_DATE(dt) as noted_date,
                                                    CAST(AverageTemperature AS DOUBLE) Average_Temperature,
                                                    CAST(AverageTemperatureUncertainty AS DOUBLE) Average_Temperature_Uncertainty,
                                                    City,
                                                    Country,
                                                    Latitude,
                                                    Longitude FROM globalTempTable
                                                    WHERE AverageTemperature IS NOT NULL
                                                    AND AverageTemperatureUncertainty IS NOT NULL
                                                    AND Country = 'United States'""")

    write_to_redshift(spark, cleansed_global_temp_df,"TEMPERATURES")


def main():
    """
        - Gets database details from the configuration file `dwh.cfg`
        - Extablishes connection with the sparkify database and gets cursor to it
        - Drops the tables if exists
        - Create tables
        - Finally closes the connection
    """

    key_id = config["AWS"]["AWS_ACCESS_KEY_ID"]
    secret_access_key = config["AWS"]["AWS_SECRET_ACCESS_KEY"]
    region = config["AWS"]["REGION"]
    spark = create_spark_session(key_id,secret_access_key, region)
    print("Spark Session Created...")

    json_file = str(config['DESC_JSON_FILE']['SAS_DESC_JSON_OPT'])
    #json_file = config['S3']['DESC_JSON_FILE']
    ###
    ### S3 paths of Input data to be processed
    ###
    json_file = config['S3']['DESC_JSON_FILE']
    sas_data = config['S3']['SAS_DATA']
    us_demographs = config['S3']['US_CITY_DEMOGRAPHICS']
    global_temp = config['S3']['GLOBAL_TEMP']
    airport_codes = config['S3']['AIRPOT_CODES']


    #loading_I94_code_tables(spark,json_file)
    loading_immgration_table(spark, sas_data)
    loading_us_demographics_table(spark,us_demographs)
    loading_airports_table(spark,airport_codes)
    loading_temperatures_table(spark,global_temp)
    
    print("###################################################")
    print("###  Loading Completed to Redshift Coompleted   ###")
    print("###################################################")



if __name__ == "__main__":
    main()

