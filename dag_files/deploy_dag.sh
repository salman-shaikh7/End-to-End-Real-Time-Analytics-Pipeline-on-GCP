# To deploy airflow dag to composer we need to copy dag python file to dag's folder using gutils 
gsutil cp dag_files/dag_python_file.py gs://us-central1-my-composer-env-0d31e5b0-bucket/dags
