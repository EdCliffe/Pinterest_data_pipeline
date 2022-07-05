from kafka import KafkaConsumer
import boto3 
from datetime import datetime
import uuid
import json
from json import loads

# create our consumer to retrieve the message from the topics
data_batch_consumer = KafkaConsumer(
    bootstrap_servers="localhost:9092",    
    value_deserializer=lambda message: loads(message),
    auto_offset_reset="earliest" # This value ensures the messages are read from the beginning 
)

data_batch_consumer.subscribe(topics=["PinterestTopic"])

s3_client = boto3.client('s3')
i = 1
for msg in data_batch_consumer:
    message_id = i
    with open("message.json", "w") as outfile:
        # json.dump("PinterestTopic=%s,Message=%s"%(msg.topic,msg.value), outfile)
        json.dump(msg.value, outfile)
    response = s3_client.upload_file("message.json" , 'pinbucket2', f"user_post_{i}.json")
    i= i+1
# Need to sort out aws access keys and so on...
