# This scripts needs to be executed from env like virtual machine on cloud or cloud shell terminal 
# or airflow environment. 
# This enviornment service account need to have access/permission to publish message

from google.cloud import pubsub_v1
publisher = pubsub_v1.PublisherClient()

project_id = "myprojectid7028" # Project id
topic_id = "my_topic" #Topic 

topic_path = publisher.topic_path(project_id, topic_id) 

future = publisher.publish(topic_path, data="message_in_bytes") # This will publish message to pub/sub

print(f"Published message ID: {future.result()}") # return message id after publishing message.