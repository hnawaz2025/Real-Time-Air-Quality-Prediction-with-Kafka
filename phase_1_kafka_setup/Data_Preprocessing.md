Data Preprocessing Strategy

Data Source
Dataset: air-streamed.csv
Contains air quality sensor data, including CO(GT), NOx (GT), C6H6(GT).
Columns like Unnamed: 15 and Unnamed: 16 were excluded during loading as they are irrelevant.

Preprocessing Steps
Missing Value Handling:

1. Replaced -200 values (indicating missing data) with pd.NA.
2. Applied forward-fill (ffill) and backward-fill (bfill) to impute missing values.
3. For numeric columns with remaining missing values, filled them with the column mean.

Data Type Conversion: Converted non-date/time columns to numeric types using pd.to_numeric, ensuring invalid entries are coerced to NaN.

Timestamp Creation: Combined the Date and Time fields into a single Timestamp column using pd.to_datetime.
Ensured consistent formatting (%Y-%m-%d %H:%M:%S) for downstream compatibility.

Row-by-Row Serialization: Each row was converted into a dictionary (row.to_dict()), serialized to JSON, and sent to Kafka.
Timestamp values were formatted as strings for easier parsing by downstream consumers.

Real-Time Simulation: Used time.sleep(1) to simulate hourly sensor readings, ensuring realistic streaming behavior.

Error Handling:
Producer Script (producer.py):
Wrapped row processing in a try-except block to catch errors during serialization or Kafka message production.
Logged errors using Python’s built-in logging module for debugging.

Best Practices:
Validation:
Ensured all columns were properly converted to their expected data types before streaming.

Observability:
Delivery reports were logged after each message was sent, providing visibility into Kafka message delivery success or failure.

Scalability Considerations:
Although real-time simulation used a single producer, scaling could involve partitioning data across multiple producers.