# Pinterest_data_pipeline
- Project aims to simulate the user-post pipeline used by Pinterest. User post JSONs are received, and processed using Lambda architecture, in batch and also real-time. Batch data is initially stored in the cloud, cleaned and stored locally, accessed with Presto and Cassandra.
- Real-time data is cleaned and monitored using Pyspark in micro-batches, and then sent to postgres.
- UML diagram of project: https://user-images.githubusercontent.com/94751059/177628201-85cafdf7-4fc1-4273-922f-a1c1ec0253b5.png
## Data Source to storage
- Data source: user_posting_emulation.py -an infinite posting loop, which simulates a users activity on-site and posts the JSONs to a localhost port. 
- project_pin_API.py contains an API to receive the user posts, it also sets up the kafka producer to add the posts to the topic
- consumer_to_s3.py creates the Kafka consumer, and passes all messages it finds in the topic to an s3 bucket.
## Access and Transform
- s3_to_spark_connector.py then retrieves the data from the s3 bucket and passes to spark, with some initial batch data cleaning:
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
![Screenshot from 2022-07-07 21-46-33](https://user-images.githubusercontent.com/94751059/181308713-b64c5569-6307-4882-8241-6cc7778180f5.png)
## Stream processing
- The userposts are passed from the kafka topic to pyspark. Basic data processing is applied, and a function is applied, which gives live feedback on whether userposts are the result of errors (containing null values). 
- The processed microbatches are then stored in a local postgres database.
## Stream monitoring
- Prometheus and grafana are then used to scrape metrics from postgres, and display dashboards. 
![Screenshot from 2022-07-26 18-52-42](https://user-images.githubusercontent.com/94751059/181308236-183dcf39-a9c3-4668-82e9-2b00c44e021e.png)
