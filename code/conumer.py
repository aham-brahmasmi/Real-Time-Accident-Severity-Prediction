# This file is uploaded on Jupyter notebok of AWS Sagemaker
import boto3
import pickle
import sagemaker
from kafka import KafkaConsumer
import pandas as pd
import json
import time
from datetime import datetime
from io import StringIO
import xgboost as xgb

# Set up the AWS session and S3 client
sagemaker_session = sagemaker.Session()
s3_client = boto3.client('s3')

# Configuration details
bucket = 'injurybucket'
model_file = 'XGBoost.pkl'
role = 'AmazonSageMaker-ExecutionRole-20240813T171036'
kafka_bootstrap_servers = ['3.89.212.127:9092']
kafka_topic = 'project-topic'
kafka_group_id = 'my_cdac'
# Load the Model from S3
s3_client.download_file(bucket, f'model/{model_file}', model_file)
with open(model_file, 'rb') as f:
    model = pickle.load(f)

# Create Kafka Consumer
consumer = KafkaConsumer(
    kafka_topic,
    bootstrap_servers=kafka_bootstrap_servers,
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id=kafka_group_id
)

# Function to map numeric predictions to injury types
def decode_predict(prediction):
    mapping = {
        0: 'FATAL',
        1: 'INCAPACITATING INJURY',
        2: 'NO INDICATION OF INJURY',
        3: 'NONINCAPACITATING INJURY',
        4: 'REPORTED, NOT EVIDENT'
    }
    return mapping.get(prediction, 'UNKNOWN')

def clean_and_convert(value):
    # Strip any unwanted characters (e.g., quotation marks, spaces)
    cleaned_value = value.strip().replace('"', '')

    # Convert to int or float
    if '.' in cleaned_value:  # If the value has a decimal point, it's a float
        return float(cleaned_value)
    else:
        return int(cleaned_value)

def make_prediction(data_str, model):
    try:
        # Define the expected order of features
        feature_names = ['POSTED_SPEED_LIMIT', 'TRAFFIC_CONTROL_DEVICE', 'DEVICE_CONDITION', 'WEATHER_CONDITION',
                         'LIGHTING_CONDITION', 'FIRST_CRASH_TYPE', 'TRAFFICWAY_TYPE', 'ALIGNMENT', 'ROADWAY_SURFACE_COND', 
                         'ROAD_DEFECT', 'CRASH_TYPE', 'DAMAGE', 'PRIM_CONTRIBUTORY_CAUSE', 'SEC_CONTRIBUTORY_CAUSE',
                         'NUM_UNITS', 'INJURIES_FATAL', 'INJURIES_INCAPACITATING', 'CRASH_HOUR', 'CRASH_DAY_OF_WEEK',
                         'CRASH_MONTH'] 
        # Split the string into a list of strings
        data_list = data_str.split(',')
        
        # Convert the list of strings to a list of numbers (ints and floats)
        data_numeric = [clean_and_convert(value) for value in data_list]
        
        # Convert the list to a DataFrame with correct column names
        df = pd.DataFrame([data_numeric], columns=feature_names)
        
        # Convert DataFrame to DMatrix if using XGBoost
        d_matrix = xgb.DMatrix(data=df)
        
        # Make prediction
        prediction = model.predict(d_matrix)[0]
        return decode_predict(prediction)

    except ValueError as e:
        print(f"Error in prediction: {e}")
        return None

# Function to store predictions in S3
def store_to_s3(predictions_df, bucket, s3_client):
    timestamp = datetime.now().strftime('%Y-%m-%d-%H-%M-%S-%f')
    filename = f'predicted_output/prediction_{timestamp}.csv'
    
    # Convert DataFrame to CSV
    csv_buffer = StringIO()
    predictions_df.to_csv(csv_buffer, index=False)
    
    # Upload CSV to S3
    s3_client.put_object(
        Bucket=bucket,
        Key=filename,
        Body=csv_buffer.getvalue(),
        ContentType='text/csv'
    )
    print(f'Stored prediction to s3://{bucket}/{filename}')

# Polling and prediction loop, processing 1 row per batch
batch_size = 5

while True:
    try:
        messages = []
        
        # Collect messages in a batch
        for i, message in enumerate(consumer):
            data = message.value.decode('utf-8')  # Assuming string format from Kafka
            messages.append(data)
            
            if (i + 1) % batch_size == 0:
                break

        # Process the batch and make predictions
        predictions = []
        for data in messages:
            try:
                prediction = make_prediction(data, model)
                predictions.append(prediction)
            except ValueError as e:
                print(f"Error in prediction: {e}")
                predictions.append(None)  # Append None if there's an error

        # Prepare DataFrame for predictions
        predictions_df = pd.DataFrame({'data': messages, 'prediction': predictions})
        
        # Store predictions to S3
        store_to_s3(predictions_df, bucket, s3_client)
        
        time.sleep(1)  # Wait for 1 second before processing the next batch

    except Exception as e:
        print(f"Error in Kafka polling or processing: {e}")
        time.sleep(5)  # Pause before retrying to avoid rapid loops
