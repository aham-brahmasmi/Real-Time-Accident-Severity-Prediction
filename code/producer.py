import boto3
from kafka import KafkaProducer
import json

# Initialize S3 client
s3 = boto3.client('s3')

# Define your S3 bucket and file details
bucket_name = 'injurybucket'
file_key = 'input/test1.csv'

# Read data from S3
obj = s3.get_object(Bucket=bucket_name, Key=file_key)
data = obj['Body'].read().decode('utf-8').splitlines()

# Initialize Kafka producer
producer = KafkaProducer(bootstrap_servers='3.89.212.127:9092',
                         value_serializer=lambda v: json.dumps(v).encode('utf-8'))

# Publish data to Kafka Topic
for row in data:
    producer.send('project-topic', row)
    producer.flush()

print("Data published to Kafka topic.")
