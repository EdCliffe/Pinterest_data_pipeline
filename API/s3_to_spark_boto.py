# submit command 
# spark-submit --packages com.datastax.spark:spark-cassandra-connector-assembly_2.12:3.2.0 ~/Work/Packages/Pinterest_data_pipeline/API/s3_to_spark_boto.py

from pyspark.sql import SparkSession
from pyspark import SparkContext, SparkConf
import os
import multiprocessing
import pandas as pd
import boto3
from json import loads 
from json import dumps 

#adding the packages required to get data form S3
#adding aws-java-sdk and aws hadoop  from the Maven configuration
# os.environ["PYSPARK_SUBMIT_ARGS"] = "--packages com.amazonaws:aws-java-sdk-s3:1.12.196,org.apache.hadoop:hadoop-aws:3.3.1 pyspark-shell"
aws_access_key_id = os.environ["AWS_ACCESS_KEY_ID"]
aws_secret_access_key = os.environ["AWS_SECRET_ACCESS_KEY"]

#creating our Spark configuration 
conf = SparkConf() \
    .setAppName('S3toSpark') \
    .setMaster(f"local[{multiprocessing.cpu_count()}]") \
    .set("spark.eventLog.enabled", False) \
    .set("fs.s3a.access.key", aws_access_key_id  ) \
    .set("fs.s3a.secret.key", aws_secret_access_key) \
    .set("fs.s3a.endpoint", "s3.amazonaws.com") \
    .setExecutorEnv(pairs=[("VAR3", "value3"), ("VAR4", "value4")]) \
    .setIfMissing("spark.executor.memory", "1g")   # Setting memory if this setting was not set previously 


s3 = boto3.resource('s3') 
bucket=s3.Bucket('pin-API') 
# Adding the packages required to get data from S3  
# os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages com.datastax.spark:spark-cassandra-connector-assembly_2.12:3.2.0 S3_to_Spark.py pyspark-shell' 
spark = SparkSession.builder.master("local").appName("testapp").getOrCreate() 
sc = spark.sparkContext 
#
# Processing notes
# Make follower count a real number
# Make tags list a real list
json_list = [] 
for i in range(1,10):
    obj = s3.Object(bucket_name='pinbucket2', key=f'user_post_{i}.json').get() 
    obj_string_to_json = obj["Body"].read().decode('utf-8') 
    data = dumps(obj_string_to_json).replace("'", '"').rstrip('"').lstrip('"')
    data = data[40::]
    json_list.append(data) 
    print(json_list)

df = spark.read.option("mode", "PERMISSIVE").option("columnNameOfCorruptRecord", "_corrupt_record").json(sc.parallelize(json_list))
df.show(n=20,truncate=300)