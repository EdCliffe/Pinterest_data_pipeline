# submit command 
# spark-submit --packages com.amazonaws:aws-java-sdk-s3:1.12.196,org.apache.hadoop:hadoop-aws:3.3.1 ~/Work/Packages/Pinterest_data_pipeline/API/s3_to_spark_connector.py


from pyspark.sql import SparkSession
from pyspark import SparkContext, SparkConf
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
df.show(truncate=False)

# make taglist a list, rather than string?

# df.withColumn("address",
#   regexp_replace($"address", "Rd", "Road"))
#   .show()

# df.select(regexp_replace('str', r'(\d+)', '--').alias('d')).collect())

# df.withColumn("tag_list", col("tag_list").cast("list"))

# name file by unique ID
# make the follower count an actual number
# could group them by category