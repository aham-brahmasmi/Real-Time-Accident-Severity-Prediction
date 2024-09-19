from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf
from pyspark.sql.types import StringType, IntegerType, FloatType
from kafka import KafkaConsumer
import boto3
import pickle
import json
from io import StringIO
import pandas as pd
from datetime import datetime
import xgboost as xgb
import time

# Create Spark session
spark = SparkSession.builder.appName("InjuryPrediction").getOrCreate()

# Set up the AWS session and S3 client
s3_client = boto3.client('s3')

# Configuration details
bucket = 'injurybucket'
model_file = 'XGBoost.pkl'
kafka_bootstrap_servers = '3.89.212.127:9092'
kafka_topic = 'project-topic'
role = 'AmazonSageMaker-ExecutionRole-20240813T171036'

# Load the Model from S3
s3_client.download_file(bucket, f'model/{model_file}', model_file)
with open(model_file, 'rb') as f:
    model = pickle.load(f)

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

# Function to clean and convert input data
def clean_and_convert(value):
    cleaned_value = value.strip().replace('"', '')
    if '.' in cleaned_value:
        return float(cleaned_value)
    else:
        return int(cleaned_value)

# Define PySpark UDF for prediction
def make_prediction(data_str):
    feature_names = ['POSTED_SPEED_LIMIT', 'TRAFFIC_CONTROL_DEVICE', 'DEVICE_CONDITION', 'WEATHER_CONDITION',
                     'LIGHTING_CONDITION', 'FIRST_CRASH_TYPE', 'TRAFFICWAY_TYPE', 'ALIGNMENT', 'ROADWAY_SURFACE_COND', 
                     'ROAD_DEFECT', 'CRASH_TYPE', 'DAMAGE', 'PRIM_CONTRIBUTORY_CAUSE', 'SEC_CONTRIBUTORY_CAUSE',
                     'NUM_UNITS', 'INJURIES_FATAL', 'INJURIES_INCAPACITATING', 'CRASH_HOUR', 'CRASH_DAY_OF_WEEK',
                     'CRASH_MONTH'] 
    try:
        data_list = data_str.split(',')
        data_numeric = [clean_and_convert(value) for value in data_list]
        df = pd.DataFrame([data_numeric], columns=feature_names)
        
        # Convert DataFrame to DMatrix for XGBoost
        d_matrix = xgb.DMatrix(data=df)
        prediction = model.predict(d_matrix)[0]
        return decode_predict(prediction)
    except Exception as e:
        print(f"Prediction error: {e}")
        return 'UNKNOWN'

# Register UDF in Spark
predict_udf = udf(make_prediction, StringType())

# Read from Kafka using Spark Streaming
kafka_df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("subscribe", kafka_topic) \
    .option("startingOffsets", "earliest") \
    .load()

# Convert Kafka message to String
kafka_df = kafka_df.selectExpr("CAST(value AS STRING)")

# Add prediction column using UDF
predictions_df = kafka_df.withColumn("prediction", predict_udf(col("value")))

# Function to store predictions to S3
def store_to_s3(predictions_df):
    timestamp = datetime.now().strftime('%Y-%m-%d-%H-%M-%S-%f')
    filename = f'predicted_output/prediction_{timestamp}.csv'
    
    # Convert to Pandas DataFrame and CSV
    pandas_df = predictions_df.toPandas()
    csv_buffer = StringIO()
    pandas_df.to_csv(csv_buffer, index=False)

    # Upload CSV to S3
    s3_client.put_object(
        Bucket=bucket,
        Key=filename,
        Body=csv_buffer.getvalue(),
        ContentType='text/csv'
    )
    print(f'Stored prediction to s3://{bucket}/{filename}')

# Write Stream to Console for Debugging and S3 for Final Predictions
query = predictions_df \
    .writeStream \
    .outputMode("append") \
    .foreachBatch(lambda batch_df, _: store_to_s3(batch_df)) \
    .start()

# Wait for the stream to finish
query.awaitTermination()
