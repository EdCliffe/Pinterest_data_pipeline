# Run command
# spark-submit --packages com.amazonaws:aws-java-sdk:1.7.4,org.apache.hadoop:hadoop-aws:2.7.7 --master spark://MASTER_DNS:7077 ~/Work/Packages/Pinterest_data_pipeline/API/spark_config.py


import multiprocessing
from pyspark import SparkContext, SparkConf
import pyspark
import os
import boto3
aws_access_key_id = os.environ["AWS_ACCESS_KEY_ID"]
aws_secret_access_key = os.environ["AWS_SECRET_ACCESS_KEY"]
cfg = (
    pyspark.SparkConf()
    # Setting the master to run locally and with the maximum amount of cpu coresfor multiprocessing.
    .setMaster(f"local[{multiprocessing.cpu_count()}]")
    # Setting application name
    .setAppName("PinBatch")
    # Setting config value via string
    .set("spark.eventLog.enabled", False)
    # Setting environment variables for executors to use
    .setExecutorEnv(pairs=[("VAR3", "value3"), ("VAR4", "value4")])
    # Setting memory if this setting was not set previously
    .setIfMissing("spark.executor.memory", "1g")
    .set("fs.s3n.awsAccessKeyId", aws_access_key_id)
    .set("fs.s3n.awsSecretAccessKey", aws_secret_access_key)
)

# # Getting a single variable
# print(cfg.get("spark.executor.memory"))
# # Listing all of them in string readable format
# print(cfg.toDebugString())



#sc._jsc.hadoopConfiguration().set("fs.s3n.awsAccessKeyId", AWS_ACCESS_KEY)
#sc._jsc.hadoopConfiguration().set("fs.s3n.awsSecretAccessKey", AWS_SECRET_KEY)

session = pyspark.sql.SparkSession.builder.config(conf=cfg).getOrCreate()
print("APP Name :"+session.sparkContext.appName)
# config_dict = {"fs.s3n.awsAccessKeyId":"aws_access_key_id",
#                "fs.s3n.awsSecretAccessKey":"aws_secret_access_key"}
# spark.sparkContext.hadoopConfiguration.set(config_dict)
# bucket = "pinbucket2"
# prefix = "0187e56d-00ba-4be5-9a0a-10c46ead7309.txt"
# filename = "s3://{}/{}".format(bucket, prefix)

s3 = boto3.resource('s3')
# get a handle on the bucket that holds your file
bucket = s3.Bucket('pinbucket2')
# get a handle on the object you want (i.e. your file)
obj = bucket.Object(key='0187e56d-00ba-4be5-9a0a-10c46ead7309.txt')
# get the object
response = obj.get()['Body']

print(response)
print(type(response))
# read the contents of the file and split it into a list of lines


df = session.read.json(response)
df.print()
# rdd = session.read.text(lines)
# col = rdd.collect()
