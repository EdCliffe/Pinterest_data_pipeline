#%%
# # Run both commands in the terminal inside you Kafka folder
# Remember to start Zookeeper first as it orchestrates Kafka Brokers
# Starting Zookeeper

# Starting Kafka
#  ./bin/kafka-server-start.sh ./config/server.properties

# Starting Zookeeper
#   ./bin/zookeeper-server-start.sh ./config/zookeeper.properties

# Make First Kafka topic
# bin/kafka-topics.sh --create --topic PinterestTopic --partitions 1 --replication-factor 1 --bootstrap-server localhost:9092

# Create producer
# kafka-console-producer.sh --topic MyFirstKafkaTopic --bootstrap-server localhost:9092 

# Create Consumer
# kafka-console-consumer.sh --topic MyFirstKafkaTopic --from-beginning --bootstrap-server localhost:9092 

# Check topics
# ./bin/kafka-topics.sh --list --bootstrap-server localhost:9092

from kafka import KafkaClient
from kafka.cluster import ClusterMetadata

# Create a connection to retrieve metadata
meta_cluster_conn = ClusterMetadata(
    bootstrap_servers="localhost:9092", # Specific the broker address to connect to
)

# retrieve metadata about the cluster
print(meta_cluster_conn.brokers())


# Create a connection to our KafkaBroker to check if it is running
client_conn = KafkaClient(
    bootstrap_servers="localhost:9092", # Specific the broker address to connect to
    client_id="Pinterest_pipeline" # Create an id from this client for reference
)

# Check that the server is connected and running
print(client_conn.bootstrap_connected())
# Check our Kafka version number
print(client_conn.check_version())

#%% Create new broker connection and add topics
from kafka import KafkaAdminClient
from kafka.admin import NewTopic
from kafka.cluster import ClusterMetadata

# Create a new Kafka client to adminstrate our Kafka broker
admin_client = KafkaAdminClient(
    bootstrap_servers="localhost:9092", 
    client_id="Kafka Administrator"
)
#%%
# topics must be pass as a list to the create_topics method
topics = []
topics.append(NewTopic(name="MLdata", num_partitions=3, replication_factor=1))
topics.append(NewTopic(name="Retaildata", num_partitions=2, replication_factor=1))

# Topics to create must be passed as a list
admin_client.create_topics(new_topics=topics)

#%% Check existing topics

admin_client.list_topics()

#%% Describe topics
#  We can pass in a list to topics to describe or describe all topics without the topics keyword argument
admin_client.describe_topics(topics=["Retaildata"])

#%% Create test data

# Lets create some test data to send using our kafka producer
ml_models = [
    {
        "Model_name": "ResNet-50",
        "Accuracy": "92.1",
        "Framework_used": "Pytorch"
    },
    {
        "Model_name": "Random Forest",
        "Accuracy": "82.7",
        "Framework_used": "SKLearn"
    }
] 

retail_data = [
    {
        "Item": "42 LCD TV",
        "Price": "209.99",
        "Quantity": "1"
    },
    {
        "Item": "Large Sofa",
        "Price": "259.99",
        "Quantity": 2
    }
]
#%%
from kafka import KafkaProducer
from json import dumps

# Configure our producer which will send data to  the MLdata topic
ml_producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    client_id="ML data producer",
    value_serializer=lambda mlmessage: dumps(mlmessage).encode("ascii")
) 

# Configure our producer which will send data to the Retaildata topic
retail_producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    client_id="ML data producer",
    value_serializer=lambda retailmessage: dumps(retailmessage).encode("ascii")
)
#%%
# Send our ml data to the MLData topic
for mlmessage in ml_models:
    ml_producer.send(topic="MLData", value=mlmessage)

# Send retail data to the Retaildata topic
for retail_message in retail_data:
    retail_producer.send(topic="Retaildata", value=retail_message)

#%% Create consumer

from kafka import KafkaConsumer
from json import loads

# create our consumer to retrieve the message from the topics
data_stream_consumer = KafkaConsumer(
    bootstrap_servers="localhost:9092",    
    value_deserializer=lambda message: loads(message),
    auto_offset_reset="earliest" # This value ensures the messages are read from the beginning 
)

data_stream_consumer.subscribe(topics=["MLData", "Retaildata"])


#%% Loop & print messages

# Loops through all messages in the consumer and prints them out individually
for message in data_stream_consumer:
    print(message)

#%% print message values
for message in data_stream_consumer:
    print(message.value)
    print(message.topic)
    print(message.timestamp)