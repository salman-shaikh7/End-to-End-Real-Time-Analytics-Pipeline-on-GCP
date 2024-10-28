#import required package to build dag
from datetime import datetime
from airflow.models.dag import DAG

#import required operators 
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.dataflow import DataflowStopJobOperator

#import rquired modules to publish messsage
import requests
import json
from google.cloud import pubsub_v1
import time 

#Function to fetch and publish message to pub/sub topic
def fetch_and_publish_message():
    duration = 20 * 60  
    start_time = time.time()

    while (time.time() - start_time) < duration:
        publisher = pubsub_v1.PublisherClient()
        project_id = "myprojectid7028"
        topic_id = "my_topic"
        topic_path = publisher.topic_path(project_id, topic_id)

        data_j=requests.get("https://randomuser.me/api/").json()

        data = data_j['results']
        message_json = json.dumps(data) # Dict to str
        message_bytes = message_json.encode("utf-8") # str to bytes (pub/sub acceptable income)

        future = publisher.publish(topic_path, data=message_bytes)
        print(f"Published message ID: {future.result()}")
        time.sleep(0.5) # wait for half a second to make 2nd api call


with DAG(
    "pubsub_dataflow_bigquery_workflow_dag",
    schedule=None,
    start_date=datetime(2021, 1, 1),
    catchup=False
) as dag:
        
    TASK_1= BashOperator(
                        task_id='Create_PubSub_Topic',
                        bash_command=
                        """
                            echo "Creating pub sub topic"
                            gcloud pubsub topics create my_topic
                            echo "pub sub topic created"
                        """
                        )
    
    TASK_2= BashOperator(
                        task_id='Create_BigQuery_Dataset',
                        bash_command=
                        """
                        echo "Creating BigQuery Dataset"
                        bq mk --location=US customer_dataset
                        echo "Created BigQuery Dataset"
                        """
                        )
    
    TASK_3=PythonOperator(
                        task_id='Publish_message_to_pub_sub',
                        python_callable=fetch_and_publish_message
                        )
    
    TASK_4=BashOperator(
                        task_id='wait_for_pipeline_execution',
                        bash_command="""
                        echo "waiting for 1300 seconds"
                        sleep 1300
                        echo "wait finished for 1300 seconds"
                        """
                        )
    
    TASK_5=BashOperator(
                        task_id='deploy_the_pipeline',
                        bash_command="""
                        export PROJECT_ID=$(gcloud config get-value project)
                        export REGION='us-east1'
                        export BUCKET=gs://${PROJECT_ID}
                        export PIPELINE_FOLDER=${BUCKET}
                        export RUNNER=DataflowRunner
                        export PUBSUB_TOPIC=projects/${PROJECT_ID}/topics/my_topic
                        export WINDOW_DURATION=60
                        export AGGREGATE_TABLE_NAME=${PROJECT_ID}:customer_dataset.customer_insights_by_min 

                        gsutil cp gs://myprojectid7028/scripts/dataflow_job_script.py /tmp/dataflow_job_script.py 

                        python3 /tmp/dataflow_job_script.py \
                            --project=${PROJECT_ID} \
                            --region=${REGION} \
                            --staging_location=${PIPELINE_FOLDER}/staging \
                            --temp_location=${PIPELINE_FOLDER}/temp \
                            --runner=${RUNNER} \
                            --input_topic=${PUBSUB_TOPIC} \
                            --window_duration=${WINDOW_DURATION} \
                            --agg_table_name=${AGGREGATE_TABLE_NAME} &

                        echo "Pipeline Deployment Done"     
                        """
                        )   
    
    TASK_6=DataflowStopJobOperator(
                        task_id="stop_dataflow_job",
                        location="us-east1",
                        drain_pipeline=True,
                        job_name_prefix="streaming-per-minute-customer-insights-pipeline-", # this name is assigned during execution of dataflow pipeline
                    )
    
#Map Dependencies    

TASK_1>>TASK_2>>TASK_3
TASK_2>>TASK_4
TASK_2>>TASK_5
TASK_4>>TASK_6
