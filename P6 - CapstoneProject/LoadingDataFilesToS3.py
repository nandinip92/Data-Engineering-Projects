import findspark

findspark.init()
import pyspark
from pyspark import SparkConf
import configparser
import os
import pandas as pd
import ast
import boto3
from botocore.exceptions import NoCredentialsError
from typing import Optional, List
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

from LabelDescriptorToJson import LabelDescriptorToJson


config = configparser.ConfigParser()
config.read("cloud.cfg")

os.environ["AWS_ACCESS_KEY_ID"] = config["AWS"]["AWS_ACCESS_KEY_ID"]
os.environ["AWS_SECRET_ACCESS_KEY"] = config["AWS"]["AWS_SECRET_ACCESS_KEY"]


def create_spark_session(key_id, secret_access_key, region):
    """
    Funtion to create a spark session to process data
    """
    conf = (
    SparkConf()
    .set("spark.jars.packages","saurfang:spark-sas7bdat:2.1.0-s_2.11,org.apache.hadoop:hadoop-aws:2.7.2")
    .set("spark.hadoop.fs.s3a.path.style.access", True)
    .set("spark.hadoop.fs.s3a.access.key", key_id)
    .set("spark.hadoop.fs.s3a.secret.key", secret_access_key)
    .set("spark.hadoop.fs.s3a.endpoint", f"s3-{region}.amazonaws.com")
    .set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .set("com.amazonaws.services.s3.enableV4", True)
    .set("spark.driver.extraJavaOptions", "-Dcom.amazonaws.services.s3.enableV4=true")
    )
    spark = SparkSession.builder.master("local").config(conf=conf).enableHiveSupport().getOrCreate()
    return spark

def upload_to_aws(local_file, bucket, s3_file):
    s3 = boto3.client('s3')
    try:
        s3.upload_file(local_file, bucket, s3_file)
        print("Upload Successful")
        return True
    except FileNotFoundError:
        print("The file was not found")
        return False
    except NoCredentialsError:
        print("Credentials not available")
        return False


def load_sas_data(spark, fname: str, output_path: str) -> None:
    """
    Function to read the sas7bdat file and write it into parquet files
    :param spark       : spark session object created in create_spark_session()
    :param fname       : sas7bat file
    :param output_path : path write the parquet files
    """
    print("------------------------")
    print("|  Loading SAS data    |")
    print("------------------------")

    df_spark = spark.read.format("com.github.saurfang.sas.spark").load(fname)
    df_spark.printSchema()
    df_spark = df_spark.filter(col('i94mode')== 1) #Loading only few records from my local as the data is huge,Actual filter should be in ETL
    df_spark.write.mode("overwrite").partitionBy("i94yr", "i94mon").parquet(output_path.strip())


def load_global_temp_data(spark: SparkSession, fname: str, output_path: str) -> None:
    """
    Function to read Global Temperatures file,as as .csv file is huge write it into parquet files
        :param spark       : spark session object created in create_spark_session()
        :param fname       : GlobalLandTemperaturesByCity.csv file
        :param output_path : path to load the parquest files
    """
    print("--------------------------------------------")
    print("|  Uploading GlobalLandTemperatures data     |")
    print("--------------------------------------------")
    bucket_name = config['S3']['BUCKET']
    s3 = config['S3']['DEMOGRAPH']
    global_temp_df = spark.read.csv(fname,header = True).filter(col('Country') == 'United States')
    global_temp_df.write.mode("overwrite").partitionBy("Country","City").parquet("Datasets_created_by_Scripts/global_temperatures")


def load_sas_descriptors(
    input_file: str, labels: List[str], output_file: str, s3_path = False) -> None:
    """
    Calls LabelDescriptorToJson class and Creates JSON file from the `I94_SAS_Labels_Descriptions.SAS`.
    And Loads that json file into S3 if given as 4th argument
        LabelDescriptorToJson(input_file, labels, output_file)
            :param input_file  : I94_SAS_Labels_Descriptions.SAS
            :param labels      : I94 labels which are described in the I94_SAS_Labels_Descriptions.SAS
            :param output_file : filename for the JSON to be created
            :param s3_path     :
    """

    LabelDescriptorToJson(input_file, labels, output_file)

    print("-------------------------------------------------")
    print("|  Uploading Label Descriptor JSON file data     |")
    print("--------------------------------------------------")

    if s3_path:
        bucket_name = config['S3']['BUCKET']
        s3 = config['S3']['DESC_JSON']
        upload_to_aws(output_file, bucket_name, s3)



def load_us_demographics(fname: str) -> None:
    """
    Function to load US_CITIES_demographics data
        :param spark       : spark session object created in create_spark_session()
        :param fname       : us-cities-demographics.csv file
        :param output_path : path to load the .csv file
    """
    print("--------------------------------------------")
    print("|  Loading US_CITIES_DEMOGRAPHICS data     |")
    print("--------------------------------------------")
    bucket_name = config['S3']['BUCKET']
    s3 = config['S3']['DEMOGRAPH']
    upload_to_aws(fname, bucket_name, s3)



def load_airpot_codes(fname: str) -> None:
    """
    Function to load Global Temperatures data
        :param spark       : spark session object created in create_spark_session()
        :param fname       : airport-codes_csv.csv file
        :param output_path : path to load the .csv file
    """
    print("-------------------------------")
    print("|  Loading AirportCodes data  |")
    print("-------------------------------")

    bucket_name = config['S3']['BUCKET']
    s3 = config['S3']['AIRPORT']
    upload_to_aws(fname, bucket_name, s3)



def main():
    """
    Function does the following
        1. Creates Spark session to Process
        2. Processes and Loads the sas7bdat file onto S3
        3. Converts SAS Descriptor file to JSON and upload into S3
        4. Loads US_CITY_DEMOGRAPHICS.csv onto S3
        5. Loads air_port_codes_csv.csv onto S3
        6. Processes and Loads the GlobalLandTemperaturesByCity.csv file onto S3
    """
    key_id = config["AWS"]["AWS_ACCESS_KEY_ID"]
    secret_access_key = config["AWS"]["AWS_SECRET_ACCESS_KEY"]
    region = config["AWS"]["REGION"]
    spark = create_spark_session(key_id,secret_access_key, region)
    print("Spark Session Created...")

    ###
    ### All the input DATA_FILES to Load
    ###
    sas_data_file = "file:///"+os.getcwd()+config["DATA_FILES"]["SAS_DATA_PATH"]
    global_temp_csv = os.getcwd()+config["DATA_FILES"]["GLOBAL_TEMPERATURES_CSV"]
    sas_desc_file = config["DATA_FILES"]["LABEL_DESC_FILE"]
    us_demographs_csv = config["DATA_FILES"]["US_DEMOGRAPHICS_CSV"]
    airport_codes_csv = config["DATA_FILES"]["AIRPORT_CODES_CSV"]

    ### converting the values to LIST type
    labels = ast.literal_eval(config["DATA_FILES"]["LABELS"])


    ####
    #### Uncomment the following to load the processed files into local
    #### NOTE: US_Demographs and Airport_codes are not processed they are just loaded directly into S3 bucket
    #### so there is no need to run those functions
    ####
    #sas_opt = "file:///" + os.getcwd() + config["LOCAL_OPT_PATHS"]["SAS_DATA_FILES"]
    #sas_desc_json = config["DESC_JSON_FILE"]["SAS_DESC_JSON_OPT"]
    #global_temp_opt = (
    #    "file:///" + os.getcwd() + config["LOCAL_OPT_PATHS"]["GLOBAL_TEMPERATURES"]
    #)

    #load_sas_data(spark, sas_data_file, sas_opt)
    #load_global_temp_data(spark, global_temp_csv, global_temp_opt)
    #load_sas_descriptors(sas_desc_file, labels, sas_desc_json)


    sas_opt = config['S3']['SAS_DATA']
    global_temp_opt = config['S3']['GLOBAL_TEMP']
    sas_desc_json = config['DESC_JSON_FILE']['SAS_DESC_JSON_OPT']
    sas_desc_json_s3 = config['S3']['DESC_JSON_FILE']
    us_demographs_opt = config['S3']['US_CITY_DEMOGRAPHICS']
    airport_codes_opt = config['S3']['AIRPOT_CODES']

    load_sas_data(spark, sas_data_file, sas_opt)
    load_global_temp_data(spark, global_temp_csv, global_temp_opt)
    load_sas_descriptors(sas_desc_file,labels,sas_desc_json,True)
    load_us_demographics(us_demographs_csv)
    load_airpot_codes(airport_codes_csv)

    print("#############################")
    print("###  Loading Completed    ###")
    print("#############################")



if __name__ == '__main__':
    main()
