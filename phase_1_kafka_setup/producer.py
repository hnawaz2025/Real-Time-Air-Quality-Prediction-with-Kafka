import pandas as pd
from confluent_kafka import Producer
import json
import time
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

conf = {'bootstrap.servers': 'localhost:9092'}
producer = Producer(conf)
topic = 'air_quality_data'

#Load dataset
df = pd.read_csv("AirQualityUCI.csv", sep=';', decimal=',', usecols=lambda col: col not in ['Unnamed: 15', 'Unnamed: 16'])

# Cleaning and preprocess
df = df.replace(-200, pd.NA)  # Replace missing values (-200) with NA
df = df.ffill()
df = df.bfill()

numeric_cols = df.select_dtypes(include=['number']).columns
for col in numeric_cols:
        if df[col].isna().any():
            df[col] = df[col].fillna(df[col].mean())

# Combine date + time
df['Timestamp'] = pd.to_datetime(df['Date'] + ' ' + df['Time'], format='%d/%m/%Y %H.%M.%S', errors='coerce')

for col in df.columns:
    if col not in ['Date','Time','Timestamp']:
        df[col] = pd.to_numeric(df[col], errors='coerce')

def delivery_report(err, msg):
    if err is not None:
        logging.error(f"Delivery failed: {err}")
    else:
        logging.info(f"Sent: {msg.value().decode('utf-8')}")

# Produce data row-by-row
for _, row in df.iterrows():
    try:
        row_dict = row.to_dict()

        # Convert Timestamp to string format if it's not null
        if pd.notnull(row_dict['Timestamp']):
            row_dict['Timestamp'] = row_dict['Timestamp'].strftime('%Y-%m-%d %H:%M:%S')
        else:
            row_dict['Timestamp'] = None

        producer.produce(
            topic,
            key=str(row_dict['Timestamp']),
            value=json.dumps(row_dict),
            callback=delivery_report)
        
        time.sleep(1) #Adding one second delay to simulate real time data 

    except Exception as e:
        logging.error(f"Error processing row: {e}")

# Flush all messages to Kafka before ending
producer.flush()
