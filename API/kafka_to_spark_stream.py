# Submit to pyspark - run as python file in terminal
# to include postgres in spark classpath 
# ./bin/spark-shell --driver-class-path postgresql-9.4.1207.jar --jars postgresql-9.4.1207.jar
import pyspark.sql.functions as F
from pyspark.sql.functions import lit
from pyspark.sql import SparkSession
from pyspark.sql.functions import when
from pyspark.sql.functions import expr, col
from pyspark.sql.types import StructType, StructField, IntegerType, LongType, StringType, FloatType
from pyspark.streaming import StreamingContext
from pyspark.sql.functions import *
from pyspark.sql.functions import udf
import re
import os
import findspark

findspark.init()

# Download spark sql kakfa package from Maven repository and submit to PySpark at runtime. 
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.1,org.postgresql:postgresql:42.2.6 pyspark-shell' #--jars postgresql-42.2.6.jar'
# --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0,org.postgresql:postgresql:42.2.10 pyspark-shell'

# specify the topic we want to stream data from.
kafka_topic_name = "PinterestTopic"
# Specify your Kafka server to read data from.
kafka_bootstrap_servers = 'localhost:9092'

spark = SparkSession \
        .builder \
        .appName("KafkaStreaming ") \
        .getOrCreate()

# Only display Error messages in the console.
spark.sparkContext.setLogLevel("ERROR")

# Construct a streaming DataFrame that reads from topic
stream_df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
        .option("subscribe", kafka_topic_name) \
        .option("startingOffsets", "earliest") \
        .load()

# Select the value part of the kafka message and cast it to a string.
stream_df = stream_df.selectExpr("CAST(value as STRING)", "timestamp as timestamp")


# Replace empty cells with None

jsonSchema = StructType([StructField("category", StringType()),
            StructField("index", IntegerType()),
            StructField("unique_id", StringType()),
            StructField("title", StringType()),
            StructField("description", StringType()),
            StructField("follower_count", StringType()),
            StructField("tag_list", StringType()),
            StructField("is_image_or_video", StringType()),
            StructField("image_src", StringType()),
            StructField("downloaded", IntegerType()),
            StructField("save_location", StringType())])

# convert JSON column to multiple columns
stream_df = stream_df.withColumn("value", F.from_json(stream_df["value"], jsonSchema)).select(col("value.*"), "timestamp")


# Clean data ==============

# Replace tag_list with an actual list, splitting on commas
stream_df = stream_df.withColumn("tag_list", split(stream_df.tag_list, ","))


# replace empty cells with Nones
df2 = stream_df.replace({'User Info Error': None}, subset = ['follower_count']) \
                     .replace({"No Title Data Available": None}, subset = ['title']) \
                     .replace({'No description available Story format': None}, subset = ['description']) \
                     .replace({'Image src error.': None}, subset = ['image_src'])
                     
# make the follower count an actual number
# regex replacing k with 000, and m, B with 000000 & 0000000? data type to integer

def follower_count_num(count):
    if type(count) == str:
        count = re.sub('k','000', count)
        count = re.sub('M','000000', count)
        count = re.sub('B','000000000', count)
        count = int(count)
    return count # type = row


# use UDF to apply function to change value in withColumn

udf_func = udf(lambda x: follower_count_num(x),returnType=IntegerType())

df2 = df2.withColumn("follower_count",udf_func(df2.follower_count))


# Make save-location into just a file path

# "save_location": "Local save in /data/travel"} -> "save_location": "/data/travel"
# slice on 16th character using spark tool substr (slicing)

df2 = (
    df2
    .withColumn('length', F.length('save_location'))
    .withColumn('save_location', F.col('save_location').substr(F.lit(15), F.col('length')))
)
df2 = df2.drop('length')
# alter column title index to index_no


df2 = df2.withColumnRenamed("index", "index_no")

df2= df2.withColumn("downloaded", df2["downloaded"].cast("int")) \
                    .withColumn("index_no", df2["index_no"].cast("int"))

# Stream function =========================

def updateFunction(newValues, runningCount):
    if runningCount is None:
        runningCount = 0
    return sum(newValues, runningCount)


def foreach_batch_function(df, epoch_id):
    # add processing steps here
    #total_nulls = df.select(stream_df["*"]).where(stream_df['follower_count']=="User Info Error").updateStateByKey(updateFunction)
    df = df.withColumn("is_error", df.follower_count.isNull())

                            
    slidingWindows = df.withWatermark("timestamp", "1 minutes") \
                    .groupBy(
                        window(df.timestamp, "2 minutes", "1 minutes"),
                        df.is_error) \
                    .count()
# .option('user', os.environ["PGADMIN_USER"]) \
#.option('password', os.environ["PGADMIN_PASS"]) \
    slidingWindows.show(truncate = False)
    df.write \
        .mode('append') \
        .format('jdbc') \
        .option('url', f'jdbc:postgresql://localhost:5432/pinterest_streaming') \
        .option('user', 'postgres') \
        .option('password', os.environ["PGADMIN_PASS"]) \
        .option('driver', 'org.postgresql.Driver') \
        .option('dbtable', 'public.pinterest_streaming') \
        .save()

    # print(os.environ["PGADMIN_USER"])
    # df.write.jdbc('jdbc:mysql://host:5432/pinterest_streaming', 'experimental_data',
    #             mode='append',
    #             properties={'user': os.environ["PGADMIN_USER"], 'password': os.environ["PGADMIN_PASS"]})
    
    # df.show

stream2 = df2.writeStream \
    .foreachBatch(foreach_batch_function) \
    .start() \
    .awaitTermination()


# ssc = StreamingContext(stream_df.sparkContext, batchDuration=30)  # AttributeError: 'DataFrame' object has no attribute 'sparkContext'

# total_nulls = stream2.select(stream2["*"]).where(stream2['follower_count']=="User Info Error").updateStateByKey(updateFunction) # AttributeError: 'DataFrame' object has no attribute 'updateStateByKey'
# updatestatebykey requires timestamp

# Running count function

# test = stream_df.select('follower_count')

#totalnulls = stream_df.select(stream_df["*"]).where(stream_df['follower_count']=="User Info Error")  # .updateStateByKey(updateFunction)




#allows us to keep one state and update it continuously with values coming from a stream
# AttributeError: 'DataFrame' object has no attribute 'updateStateByKey'



# total nulls to date /  total non-nulls todate
# feature_df = stream_df.groupBy(stream_df.category).count() 

# saving the ID of the error, or message title


# saveAsTextFiles(prefix, [suffix]) 	Save this DStream's contents as text files. The file name at each batch interval is generated based on prefix and suffix: "prefix-TIME_IN_MS[.suffix]".


# outputting the messages to the console 
df2.writeStream \
    .format("console") \
    .outputMode("append") \
    .start() \
    .awaitTermination()
