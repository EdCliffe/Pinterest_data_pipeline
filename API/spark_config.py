# Run command
# spark-submit --packages com.amazonaws:aws-java-sdk:1.7.4,org.apache.hadoop:hadoop-aws:2.7.7 --master spark://MASTER_DNS:7077 ~/Work/Packages/Pinterest_data_pipeline/API/spark_config.py


import multiprocessing
from pyspark import SparkContext, SparkConf
import pyspark
import os
aws_access_key_id = os.environ["aws_access_key_id"]
aws_secret_access_key = os.environ["aws_secret_access_key"]
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

print("1st line is printed")

#sc._jsc.hadoopConfiguration().set("fs.s3n.awsAccessKeyId", AWS_ACCESS_KEY)
#sc._jsc.hadoopConfiguration().set("fs.s3n.awsSecretAccessKey", AWS_SECRET_KEY)

session = pyspark.sql.SparkSession.builder.config(conf=cfg).getOrCreate()
# config_dict = {"fs.s3n.awsAccessKeyId":"aws_access_key_id",
#                "fs.s3n.awsSecretAccessKey":"aws_secret_access_key"}
# spark.sparkContext.hadoopConfiguration.set(config_dict)
bucket = "pinbucket2"
prefix = "0187e56d-00ba-4be5-9a0a-10c46ead7309"
filename = "s3://{}/{}".format(bucket, prefix)

def g(x):
    return print(x)

rdd = session.SparkContext.textFile(filename)
col = rdd.collect()
for x in col:
    print(x)

print("2nd line is printed")