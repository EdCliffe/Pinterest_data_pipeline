# submit command 
# spark-submit --packages com.amazonaws:aws-java-sdk-s3:1.12.196,org.apache.hadoop:hadoop-aws:3.3.1 ~/Work/Packages/Pinterest_data_pipeline/API/s3_to_spark_connector.py
from pyspark.sql import functions as F
import pyspark.sql.types as T
from pyspark.sql.functions import split, col
from pyspark.sql import SparkSession
from pyspark import SparkContext, SparkConf
import re
import os
# Adding the packages required to get data from S3  
# os.environ["PYSPARK_SUBMIT_ARGS"] = "--packages com.amazonaws:aws-java-sdk-s3:1.12.196,org.apache.hadoop:hadoop-aws:3.3.1 pyspark-shell"

# Creating our Spark configuration
conf = SparkConf() \
    .setAppName('S3toSpark') \
    .setMaster('local[*]')

sc=SparkContext(conf=conf)

# Configure the setting to read from the S3 bucket

aws_access_key_id = os.environ["AWS_ACCESS_KEY_ID"]
aws_secret_access_key = os.environ["AWS_SECRET_ACCESS_KEY"]
hadoopConf = sc._jsc.hadoopConfiguration()
hadoopConf.set('fs.s3a.access.key', aws_access_key_id)
hadoopConf.set('fs.s3a.secret.key', aws_secret_access_key)
hadoopConf.set('spark.hadoop.fs.s3a.aws.credentials.provider', 'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider') # Allows the package to authenticate with AWS

# Create our Spark session
spark=SparkSession(sc)

# Read from the S3 bucket
df = spark.read.json("s3a://pinbucket2/user_post_190.json") # You may want to change this to read csv depending on the files your reading from the bucket

# make operations on data - this could be useful https://stackoverflow.com/questions/29109916/updating-a-dataframe-column-in-spark

# Replace tag_list with an actual list, splitting on commas
df2 = df.withColumn("tag_list", split(df.tag_list, ","))



# df2.printSchema()

# make the follower count an actual number

# regex replacing k with 000, and m, B with 000000 & 0000000? data type to integer

def follower_count_num(count):
    count = re.sub('k','000', count)
    count = re.sub('M','000000', count)
    count = re.sub('B','000000000', count)
    return print(count)


# create rdd, and do regex as map function
df3 = df2.select("follower_count")
rdd = df3.rdd
rdd2 = rdd.map(lambda x: follower_count_num(str(x)))

# take value produced by rdd and update DF

df2 = df2.withColumn("follower_count", follower_df)
df2.show(truncate=False)

# Make save-location into just a file path

# Save location as a path, string
# "save_location": "Local save in /data/travel"} -> "save_location": "/data/travel"