# Pinterest_data_pipeline
## Data Source to storage
- Two pieces of code are used as a starting point. An infinite posting loop, found in the user posting folder, which simulates a users activity on-site and posts it to a localhost port. The other is project_pin_API built using FastAPI which registers this users information as it is posted.
- project_pin_API.py also sets up a Kafka producer and sends the message to the topic.
- consumer_to_s3.py creates the Kafka consumer, and passes all messages it finds in the topic to an s3 bucket.
## Access and Transform
- s3_to_spark_connector.py then retrieves the data from the s3 bucket and passes to spark, with some initial data cleaning:
> - Convert the follower_count into a real number. (ie 5k -> 5000) 
> - Change the file path into usable path. "File path is /data/post" -> "/data/post"
> - Convert the list stored as a string, into an actual list. "one, two, three" -> [one, two, three]
- The cleaned userpost is then stored in Cassandra using the datastax connector, and the final line of the s3_to_spark_connector.py.
## Presto
- Presto is installed, connected and used to query the cassandra database.
> - image: https://user-images.githubusercontent.com/94751059/177627935-2d697ead-7334-4bad-8ec4-3de2372d785f.png
## Automation
- Airflow is used to run s3-clean-to-cassandra job on a timer. The airflow file "pin-dag" is included.
> - image: https://user-images.githubusercontent.com/94751059/177627469-fc288a39-486f-4669-869a-96d199798f0b.png
- Prometheus and Grafana are connected to monitor
