# Submit to pyspark - run as python file in terminal
from pyspark.sql import SparkSession
import os
import findspark
findspark.init()


# Download spark sql kakfa package from Maven repository and submit to PySpark at runtime. 
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.1 pyspark-shell'
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
stream_df = stream_df.selectExpr("CAST(value as STRING)")

jsonSchema = StructType([StructField("index", IntegerType()),
                        StructField("unique_id", StringType()),
                        StructField("title", StringType()),
                        StructField("description", StringType()),
                        StructField("follower_count", StringType()),
                        StructField("tag_list", StringType()),
                        StructField("poster_name", StringType()),
                        StructField("is_image_or_video", StringType()),
                        StructField("image_src", StringType()),
                        StructField("downloaded", IntegerType()),
                        StructField("save_location", StringType()),
                        StructField("category", StringType()),
                        ])

# convert JSON column to multiple columns
stream_df = stream_df.withColumn("value", F.from_json(stream_df["value"], jsonSchema)).select(col("value.*"))

# replace empty cells with Nones
stream_df = stream_df.replace({'User Info Error': None}, subset = ['follower_count']) \
                     .replace({"No Title Data Available": None}, subset = ['title']) \
                     .replace({'No description available Story format': None}, subset = ['description']) \
                     .replace({'Image src error.': None}, subset = ['image_src']) \
                     .replace({"N,o, ,T,a,g,s, ,A,v,a,i,l,a,b,l,e" : None}, subset = ['tag_list'])


    # Process data, extract 2 metrics, clean a bit, save to disk? save to s3? add a checkpoint?

    # time window, posts per minute received

# Running count function

def updateFunction(newValues, runningCount):
    if runningCount is None:
        runningCount = 0
    return sum(newValues, runningCount)

stream_df.updateStateByKey(updateFunction) #allows us to keep one state and update it continuously with values coming from a stream

# Convert blank columns to null value
stream_df=stream_df.select([when(col(c)=="",None).otherwise(col(c)).alias(c) for c in stream_df.columns])


windowedNullCounts = stream_df.reduceByKeyAndWindow(lambda x, y: x + y, lambda x, y: x - y, 30, 10)

    # count values = None in rolling window of 1 minute

    # total nulls to date /  total non-nulls todate
feature_df = stream_df.groupBy(stream_df.category).count() 

    # saving the ID of the error, or message title


# saveAsTextFiles(prefix, [suffix]) 	Save this DStream's contents as text files. The file name at each batch interval is generated based on prefix and suffix: "prefix-TIME_IN_MS[.suffix]".


# outputting the messages to the console 
stream_df.writeStream \
    .format("console") \
    .outputMode("append") \
    .start() \
    .awaitTermination()
