from datetime import datetime
from airflow.models.dag import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

def test_py_funtion():
    print("In python testing function")

with DAG(
    "Testing_Dag_for_bucket_dataset_id",
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
    
    
    


TASK_1>>TASK_2