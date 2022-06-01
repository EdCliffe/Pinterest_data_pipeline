# Pinterest_data_pipeline
## First steps
- Two pieces of code are used as a starting point. An infinite posting loop, found in the user posting folder, which simulates a users activity on-site and posts it to a localhost port. The other is project_pin_API built using FastAPI which registers this users information as it is posted.
- project_pin_API.py also sets up a Kafka producer and sends the message to the topic.
- consumer_to_s3.py creates the Kafka consumer, and passes all messages it finds in the topic to an s3 bucket.
- s3_to_spark_boto.py then retrieves the data from the s3 bucket and passes to spark, with some initial data cleaning.
