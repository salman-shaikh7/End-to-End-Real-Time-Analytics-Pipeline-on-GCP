import requests
import json
from google.cloud import pubsub_v1
import time 

#This function will fetch from API with delay of 0.5 sec gap.
#This will also publish message to pub/sub Topic
#Can be executed on airflow env which have necessary IAM access.

def fetch_and_publish_message():
    duration = 5 * 60  
    start_time = time.time()

    while (time.time() - start_time) < duration:
        publisher = pubsub_v1.PublisherClient()
        project_id = "myprojectid7028"
        topic_id = "my_topic"
        topic_path = publisher.topic_path(project_id, topic_id)

        data_j=requests.get("https://randomuser.me/api/").json()

        data = data_j['results']
        message_json = json.dumps(data)
        message_bytes = message_json.encode("utf-8")

        future = publisher.publish(topic_path, data=message_bytes)
        time.sleep(0.5)
        print(f"Published message ID: {future.result()}")

if __name__ == '__main__':
  fetch_and_publish_message()
