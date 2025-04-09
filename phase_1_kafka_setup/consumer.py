from confluent_kafka import Consumer, KafkaException
import json
import pandas as pd
import logging
import os

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

conf = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'air_quality_group',
    'auto.offset.reset': 'earliest'
}
consumer = Consumer(conf)
consumer.subscribe(['air_quality_data'])

messages = []
is_first_batch = True  
try:
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue

        value = msg.value().decode('utf-8')
        record = json.loads(value)
        messages.append(record)
        logging.info(f"Consumed: {record.get('Timestamp', 'N/A')}")
        
        # Save messages to CSV when there are at least 10 messages
        if len(messages) >= 10:
            df = pd.DataFrame(messages)
            
            # Check if the CSV file already exists
            if not os.path.exists("air_quality_streamed.csv"):
                # Write the header for the first batch
                df.to_csv("air_quality_streamed.csv", mode='w', header=True, index=False)
                logging.info(f"Saved {len(messages)} records with header")
            else:
                # Append without the header for subsequent batches
                df.to_csv("air_quality_streamed.csv", mode='a', header=False, index=False)
                logging.info(f"Appended {len(messages)} records")
            
            messages.clear()  

except KeyboardInterrupt:
    logging.info("Interrupted by user.")
except Exception as e:
    logging.error(f"Error consuming messages: {e}")
finally:
    consumer.close()

