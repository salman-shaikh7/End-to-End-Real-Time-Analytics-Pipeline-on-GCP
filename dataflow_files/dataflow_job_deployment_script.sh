#Execute this script from Airflow or any env to run the dataflow pipeline
#Preq : your dataflow_job_script.py need to be in "gs://myprojectid7028/scripts/" folder

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

echo "Done"     