Kafka Setup 

Environment Configuration
System Details:
Operating System: macOS Sequoia
Java Version: JDK 17 (Kafka requires Java 11+)
Python Version: Python 3.10
Kafka Python Library: confluent-kafka-python

Kafka Installation:
Downloaded Apache Kafka binary (kafka_2.13-4.0.0.tgz) from the official website.
Extracted the binary and configured Kafka in KRaft mode (no Zookeeper required).

KRaft Mode Configuration
Edited the configuration file (config/kraft/server.properties) with key settings:

process.roles=broker,controller
node.id=1
controller.quorum.voters=1@localhost:9093
listeners=PLAINTEXT://localhost:9092,CONTROLLER://localhost:9093
log.dirs=/tmp/kraft-combined-logs

Formatted the storage directories using:
bin/kafka-storage.sh format -t $(bin/kafka-storage.sh random-uuid) -c config/kraft/server.properties

Started the Kafka broker with:
bin/kafka-server-start.sh config/kraft/server.properties

Topic Creation
Created a topic named air_quality_data for streaming air quality data:
bin/kafka-topics.sh --create \
  --topic air_quality_data \
  --bootstrap-server localhost:9092 \
  --partitions 1 \
  --replication-factor 1
Used a single partition for prototyping; production systems would use multiple partitions for scalability.

Producer Implementation
Key Features in producer.py:
Reads data from the UCI Air Quality dataset (AirQualityUCI.csv).

1. Preprocesses data by handling missing values and converting columns to appropriate formats.
2. Combines date and time fields into a unified timestamp for temporal consistency.
3. Sends each row as a JSON object to the Kafka topic (air_quality_data) with a one-second delay (time.sleep(1)).
4. Includes error handling for row processing and message delivery reporting via callbacks.

Example Log Output from Producer:
text
2025-04-08 21:01:00 - INFO - Sent: {"Timestamp": "2025-04-08 21:01:00", "CO(GT)": "0.7", ...}
2025-04-08 21:01:01 - ERROR - Delivery failed: Broker not available

Consumer Implementation
Key Features in consumer.py:
1. Subscribes to the topic (air_quality_data) with group ID air_quality_group.
2. Continuously polls messages from Kafka and parses them into Python dictionaries.
3. Saves batches of messages (minimum size = 10) into a CSV file (air_quality_streamed.csv).
4. Appends new records without overwriting existing data in subsequent batches.
5. Includes error handling for message consumption and file I/O operations.

Example Log Output from Consumer:
text
2025-04-08 21:02:00 - INFO - Consumed: 2025-04-08 21:01:00
2025-04-08 21:02:10 - INFO - Saved 10 records with header

Challenges Encountered
Challenge	Resolution	Best Practice
Kafka switched to KRaft mode (no Zookeeper)	Followed official documentation for KRaft setup	Start with minimal configuration first
Missing values in UCI dataset	Implemented forward-fill/backward-fill and mean imputation	Validate data quality before streaming
Real-time simulation	Added one-second delay using time.sleep(1)	Use event-time watermarks in production
Consumer reprocessing old messages	Set auto.offset_reset='earliest'	Use consumer groups for offset management


