from datetime import datetime
from airflow.models.dag import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

def test_py_funtion():
    print("In python testing function")

with DAG(
    "Testing_Dag_id",
    schedule=None,
    start_date=datetime(2021, 1, 1),
    catchup=False
) as dag:
        
    TASK_1= BashOperator(
                        task_id='task_1_id',
                        bash_command="""
                        echo "Running Task 1"
                        PROJECT_ID=$(gcloud config get-value project)
                        echo $PROJECT_ID
                        """
                        )
    
    TASK_2= PythonOperator(
                        task_id='task_2_id',
                        python_callable=test_py_funtion
                        )

TASK_1>>TASK_2