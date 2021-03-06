from kafka import KafkaClient
from kafka.cluster import ClusterMetadata
from kafka import KafkaAdminClient


# Create a connection to retrieve metadata
meta_cluster_conn = ClusterMetadata(
    bootstrap_servers="localhost:9092",) # Specific the broker address to connect to)

# retrieve metadata about the cluster
print(meta_cluster_conn.brokers())


# Create a connection to our KafkaBroker to check if it is running
client_conn = KafkaClient(
    bootstrap_servers="localhost:9092", # Specific the broker address to connect to
    client_id="PinBroker" # Create an id from this client for reference
)

# Check that the server is connected and running
print(client_conn.bootstrap_connected())
# Check our Kafka version number
print(client_conn.check_version())

admin_client = KafkaAdminClient(
    bootstrap_servers="localhost:9092", 
    client_id="Kafka Administrator"
)
# check topics
print(admin_client.list_topics())