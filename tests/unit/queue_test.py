import boto3

sqs = boto3.client(
    "sqs",
    endpoint_url="http://node2:9324",
    region_name="us-east-1",
    aws_access_key_id="x",
    aws_secret_access_key="x"
)

resp = sqs.receive_message(
    QueueUrl="http://node2:9324/queue/request-queue",
    MaxNumberOfMessages=5,
    VisibilityTimeout=0
)

print(resp)