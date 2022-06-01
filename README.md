# Pinterest_data_pipeline
## First steps
- Two pieces of code are used as a starting point. An infinite posting loop, which simulates a users activity on-site and posts it to a localhost port, and an API which registers this users information as it is posted.
- project_pin_API.py sets up a Kafka producer and sends the message to the topic.
- consumer_to_s3.py creates the Kafka consumer, and passes all messages it finds in the topic to an s3 bucket.
- s3_to_spark_boto.py then retrieves the data from the s3 bucket, and does some first pass data cleaning in spark.
