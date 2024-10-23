# check env service account have IAM access to create pub/Sub
echo "Creating pub sub topic"
gcloud pubsub topics create my_topic
echo "pub sub topic created"

