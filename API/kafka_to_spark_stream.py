# Submit to pyspark - run as python file in terminal
from pyspark.sql import SparkSession

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

# outputting the messages to the console 
stream_df.writeStream \
    .format("console") \
    .outputMode("append") \
    .start() \
    .awaitTermination()

    # Process data, extract 2 metrics, clean a bit, save to disk? save to s3? add a checkpoint?

    # time window, posts per minute received

# Running count function
def updateFunction(newValues, runningCount):
    if runningCount is None:
        runningCount = 0
    return sum(newValues, runningCount)

stream_df.updateStateByKey(updateFunction) #allows us to keep one state and update it continuously with values coming from a stream


    # post per minute errors, and % total nulls to date
    # saving the ID of the error, or message title

    # save a list of null posts, save to file

# saveAsTextFiles(prefix, [suffix]) 	Save this DStream's contents as text files. The file name at each batch interval is generated based on prefix and suffix: "prefix-TIME_IN_MS[.suffix]".
