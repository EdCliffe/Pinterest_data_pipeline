# submit command, without hash, in Programs / spark / bin
# spark-submit --packages com.amazonaws:aws-java-sdk-s3:1.12.196,org.apache.hadoop:hadoop-aws:3.3.1,com.datastax.spark:spark-cassandra-connector_2.12:3.2.0 ~/Work/Packages/Pinterest_data_pipeline/API/s3_to_spark_connector.py

from pyspark.sql import functions as F
import pyspark.sql.types as T
from pyspark.sql.functions import split, col
from pyspark.sql import SparkSession
from pyspark import SparkContext, SparkConf
from pyspark.sql.types import IntegerType, Row
from pyspark.sql.types import StringType
from pyspark.sql.functions import udf
import re
import os

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

# Read from the S3 bucket - iterate through contents
# filename = f"user_post_{i}"

df = spark.read.json(f"s3a://pinbucket2/*.json") # You may want to change this to read csv depending on the files your reading from the bucket

# Replace tag_list with an actual list, splitting on commas
df2 = df.withColumn("tag_list", split(df.tag_list, ","))

# make the follower count an actual number
# regex replacing k with 000, and m, B with 000000 & 0000000? data type to integer

def follower_count_num(count):
    count = re.sub('k','000', count)
    count = re.sub('M','000000', count)
    count = re.sub('B','000000000', count)
    count = int(count)
    return count # type = row

# use UDF to apply function to change value in withColumn

udf_func = udf(lambda x: follower_count_num(x),returnType=IntegerType())

df2 = df2.withColumn("follower_count",udf_func(df2.follower_count))


# Make save-location into a file path
# "save_location": "Local save in /data/travel"} -> "save_location": "/data/travel"

df2 = (
    df2
    .withColumn('length', F.length('save_location'))
    .withColumn('save_location', F.col('save_location').substr(F.lit(15), F.col('length')))
)
df3 = df2.drop('length')

# alter column title index to index_no, SQL keyword
df3 = df3.withColumnRenamed("index", "index_no")
df3.show(truncate=False)

#Created Cassandra keyspace and table in cqlsh

# CREATE KEYSPACE pinkeyspace
#   WITH REPLICATION = { 
#    'class' : 'SimpleStrategy', 
#    'replication_factor' : 1 
#   };

# CREATE TABLE pinkeyspace.userposts (category text, index int, unique_id UUID PRIMARY KEY, title text, description text, follower_count int, tag_list list<text>, is_image_or_video text, image_src text, downloaded int, save_location text);

#Write to cassandra table

df3.write.format("org.apache.spark.sql.cassandra").mode('append').options(table="userposts", keyspace="pinkeyspace").save()