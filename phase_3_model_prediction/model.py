# import joblib
# import numpy as np
# import pandas as pd
# import json
# import time
# from confluent_kafka import Consumer 
# from statsmodels.tsa.arima.model import ARIMA, ARIMAResults
# from sklearn.ensemble import RandomForestRegressor
# from sklearn.metrics import mean_absolute_error, mean_squared_error

# # Configuration
# BOOTSTRAP_SERVERS = ['localhost:9092']
# CONSUMER_TOPIC = 'air_quality_data'
# MODELS_DIR = 'models/'

# class AirQualityPredictor:
#     def __init__(self):
#         # Data buffer to store recent readings for feature engineering
#         self.data_buffer = {}  # Dictionary to store data for each pollutant
#         self.models = {}       # Dictionary to store loaded models
#         self.pollutants = ['CO(GT)', 'NOx(GT)', 'C6H6(GT)']
#         self.rf_models = {}    # Random Forest models
#         self.arima_models = {} # ARIMA models
        
#         # Initialize data buffers for each pollutant
#         for pollutant in self.pollutants:
#             self.data_buffer[pollutant] = pd.DataFrame()
        
#         # Load all models
#         self.load_models()

#     def load_models(self):
#         """Load all trained models from disk"""
#         try:
#             for pollutant in self.pollutants:
#                 # Load Random Forest model
#                 model_name = pollutant.replace('(GT)', '').lower()
#                 self.rf_models[pollutant] = joblib.load(f'{MODELS_DIR}model_{model_name}.pkl')
#                 print(f"Random Forest model for {pollutant} loaded successfully")
                
#                 # Try to load ARIMA model if available
#                 try:
#                     # For ARIMA models, we would typically save the model differently
#                     # This is a placeholder - adjust based on how you saved your ARIMA models
#                     self.arima_models[pollutant] = ARIMAResults.load(f'{MODELS_DIR}arima_{model_name}.pkl')
#                     print(f"ARIMA model for {pollutant} loaded successfully")
#                 except Exception as e:
#                     print(f"Could not load ARIMA model for {pollutant}: {e}")
#                     # Initialize a new ARIMA model with default parameters if needed
#                     # Note: This won't work for prediction until trained
                    
#         except Exception as e:
#             print(f"Error loading models: {e}")
    
#     def preprocess_data(self, new_data):
#         """
#         Preprocess incoming data for prediction
        
#         Args:
#             new_data: Dictionary or DataFrame containing new air quality reading
        
#         Returns:
#             Processed DataFrame ready for prediction
#         """
#         try:
#             # Convert to DataFrame if it's a dictionary
#             if isinstance(new_data, dict):
#                 if 'timestamp' in new_data:
#                     timestamp = pd.to_datetime(new_data['timestamp'])
#                     new_data_df = pd.DataFrame([new_data], index=[timestamp])
#                 else:
#                     new_data_df = pd.DataFrame([new_data])
#                     new_data_df.index = [pd.Timestamp.now()]
#             else:
#                 new_data_df = new_data.copy()
                
#             # Create time features
#             new_data_df['hour'] = new_data_df.index.hour
#             new_data_df['day'] = new_data_df.index.day
#             new_data_df['month'] = new_data_df.index.month
            
#             # Update data buffer for each pollutant
#             for pollutant in self.pollutants:
#                 if pollutant in new_data_df.columns:
#                     # Add the new data to the buffer
#                     if len(self.data_buffer[pollutant]) == 0:
#                         self.data_buffer[pollutant] = new_data_df[[pollutant, 'hour', 'day', 'month']]
#                     else:
#                         self.data_buffer[pollutant] = pd.concat([
#                             self.data_buffer[pollutant], 
#                             new_data_df[[pollutant, 'hour', 'day', 'month']]
#                         ])
                    
#                     # Keep only recent data (e.g., last 24 hours)
#                     # Limiting buffer size for memory efficiency
#                     if len(self.data_buffer[pollutant]) > 24:
#                         self.data_buffer[pollutant] = self.data_buffer[pollutant].iloc[-24:]
            
#             return new_data_df
            
#         except Exception as e:
#             print(f"Error in preprocessing data: {e}")
#             return None
    
#     def create_features(self, data, pollutant):
#         """
#         Create features needed for prediction
        
#         Args:
#             data: DataFrame containing recent data
#             pollutant: Target pollutant to predict
            
#         Returns:
#             DataFrame with features required by the model
#         """
#         try:
#             # Get pollutant buffer data
#             buffer = self.data_buffer[pollutant]
            
#             if len(buffer) < 3:  # Need at least 3 points for rolling stats
#                 print(f"Not enough data for {pollutant} prediction. Need at least 3 data points.")
#                 return None
            
#             # Create a copy of the latest data point
#             latest_data = data.iloc[-1:].copy()
            
#             # Add lag features
#             latest_data[f'{pollutant}_lag1'] = buffer[pollutant].iloc[-2]  # Second-to-last value
            
#             # Add rolling stats
#             latest_data[f'{pollutant}_rolling_mean3'] = buffer[pollutant].iloc[-3:].mean()
#             latest_data[f'{pollutant}_rolling_std3'] = buffer[pollutant].iloc[-3:].std()
            
#             # Select only needed features
#             feature_cols = ['hour', 'day', 'month', 
#                            f'{pollutant}_lag1', f'{pollutant}_rolling_mean3', f'{pollutant}_rolling_std3']
            
#             return latest_data[feature_cols]
            
#         except Exception as e:
#             print(f"Error creating features for {pollutant}: {e}")
#             return None
    
#     def predict_rf(self, features, pollutant):
#         """
#         Make Random Forest prediction
        
#         Args:
#             features: DataFrame with features
#             pollutant: Target pollutant
            
#         Returns:
#             prediction value
#         """
#         try:
#             # Get the model for this pollutant
#             model = self.rf_models[pollutant]
            
#             # Make prediction
#             prediction = model.predict(features)[0]
            
#             return prediction
            
#         except Exception as e:
#             print(f"Error in Random Forest prediction for {pollutant}: {e}")
#             return None
    
#     def predict_arima(self, pollutant):
#         """
#         Make ARIMA prediction
        
#         Args:
#             pollutant: Target pollutant
            
#         Returns:
#             prediction value
#         """
#         try:
#             if pollutant not in self.arima_models:
#                 print(f"No ARIMA model available for {pollutant}")
#                 return None
                
#             # Get the model
#             model = self.arima_models[pollutant]
            
#             # Make one-step forecast
#             forecast = model.forecast(steps=1)[0]
            
#             return forecast
            
#         except Exception as e:
#             print(f"Error in ARIMA prediction for {pollutant}: {e}")
#             return None
    
#     def predict_baseline(self, pollutant):
#         """
#         Make baseline prediction (previous value)
        
#         Args:
#             pollutant: Target pollutant
            
#         Returns:
#             previous value as prediction
#         """
#         try:
#             # Get the latest value from the buffer
#             if len(self.data_buffer[pollutant]) > 0:
#                 return self.data_buffer[pollutant][pollutant].iloc[-1]
#             else:
#                 return None
                
#         except Exception as e:
#             print(f"Error in baseline prediction for {pollutant}: {e}")
#             return None
    
#     def process_message(self, message):
#         """
#         Process a message from Kafka and make predictions
        
#         Args:
#             message: Kafka message object
#         """
#         try:
#             # Parse message value
#             message_value = json.loads(message.value.decode('utf-8'))
            
#             # Preprocess data
#             processed_data = self.preprocess_data(message_value)
            
#             if processed_data is None:
#                 return
            
#             # Results to store predictions
#             results = {
#                 'timestamp': processed_data.index[-1].isoformat(),
#                 'predictions': {}
#             }
            
#             # Make predictions for each pollutant
#             for pollutant in self.pollutants:
#                 if pollutant in processed_data.columns:
#                     # Create features for this pollutant
#                     features = self.create_features(processed_data, pollutant)
                    
#                     if features is not None:
#                         # Random Forest prediction
#                         rf_pred = self.predict_rf(features, pollutant)
                        
#                         # ARIMA prediction
#                         arima_pred = self.predict_arima(pollutant)
                        
#                         # Baseline prediction
#                         baseline_pred = self.predict_baseline(pollutant)
                        
#                         # Store predictions
#                         results['predictions'][pollutant] = {
#                             'actual': float(processed_data[pollutant].iloc[-1]),
#                             'random_forest': float(rf_pred) if rf_pred is not None else None,
#                             'arima': float(arima_pred) if arima_pred is not None else None,
#                             'baseline': float(baseline_pred) if baseline_pred is not None else None
#                         }
            
#             # Print predictions
#             print(json.dumps(results, indent=2))
            
#         except Exception as e:
#             print(f"Error processing message: {e}")

# def main():
#     """Main function to run the predictor"""
#     # Create predictor
#     predictor = AirQualityPredictor()
#     conf = {
#         'bootstrap.servers': 'localhost:9092',
#         'group.id': 'air_quality_group',
#         'auto.offset.reset': 'earliest'
#     }
#     # Create Kafka consumer
#     consumer = Consumer(conf)
    
#     print(f"Starting Kafka consumer, listening for messages on '{CONSUMER_TOPIC}'...")
    
#     try:
#         # Poll for new messages
#         for message in consumer:
#             # Process message
#             predictor.process_message(message)
            
#     except KeyboardInterrupt:
#         print("Stopping consumer...")
#     finally:
#         consumer.close()
#         print("Consumer closed.")


# 
# import joblib
# import pandas as pd
# import numpy as np
# from confluent_kafka import Consumer, KafkaException, KafkaError
# from sklearn.preprocessing import StandardScaler
# from sklearn.ensemble import RandomForestRegressor
# from statsmodels.tsa.arima.model import ARIMA
# import os
# import json

# POLLUTANTS = ['CO(GT)', 'NOx(GT)', 'C6H6(GT)']
# MODEL_DIR = 'models/'  # Directory to load pre-trained models
# PREDICTIONS_DIR = 'predictions/'  # Directory to save predictions
# os.makedirs(PREDICTIONS_DIR, exist_ok=True)  # Create predictions directory if it doesn't exist

# class AirQualityPredictionService:
#     def __init__(self, kafka_config, topic='air_quality_data'):
#         """Initialize the prediction service with Confluent Kafka configuration"""
#         self.consumer = Consumer(kafka_config)
#         self.topic = topic
#         self.rf_models = {}
#         self.arima_models = {}
#         self.scaler = StandardScaler()
        
#         self.load_models()

#     def load_models(self):
#         """Load pre-trained models"""
#         try:
#             for pollutant in POLLUTANTS:
#                 rf_model_path = os.path.join(MODEL_DIR, f"model_{pollutant.lower()}.pkl")
#                 arima_model_path = os.path.join(MODEL_DIR, f"arima_{pollutant.lower()}.pkl")
                
#                 if os.path.exists(rf_model_path):
#                     self.rf_models[pollutant] = joblib.load(rf_model_path)
#                 else:
#                     print(f"Random Forest model for {pollutant} not found.")
                
#                 if os.path.exists(arima_model_path):
#                     self.arima_models[pollutant] = joblib.load(arima_model_path)
#                 else:
#                     print(f"ARIMA model for {pollutant} not found.")
#         except Exception as e:
#             print(f"Error loading models: {e}")
    
#     def preprocess_data(self, data):
#         """Process raw input data and return the required feature set"""
#         try:
#             # Preprocess the data (assuming data comes in a proper format)
#             data['hour'] = data.index.hour
#             data['day'] = data.index.day
#             data['month'] = data.index.month
#             data['day_of_week'] = data.index.dayofweek
#             # Add additional features based on your model requirements
#             return data
#         except Exception as e:
#             print(f"Error processing data: {e}")
#             return None
    
#     def make_predictions(self, data):
#         """Make predictions for each pollutant"""
#         predictions = {}

#         try:
#             processed_data = self.process_data(data)

#             for pollutant in POLLUTANTS:
#                 if pollutant in processed_data.columns:
#                     # Make predictions using Random Forest
#                     rf_model = self.rf_models.get(pollutant)
#                     if rf_model:
#                         X = processed_data.drop(columns=[pollutant])  # Exclude target column
#                         rf_predictions = rf_model.predict(X)
#                         predictions[f"{pollutant}_rf_predictions"] = rf_predictions

#                     # Make predictions using ARIMA
#                     arima_model = self.arima_models.get(pollutant)
#                     if arima_model:
#                         arima_predictions = arima_model.forecast(steps=len(data))
#                         predictions[f"{pollutant}_arima_predictions"] = arima_predictions
#                 else:
#                     print(f"{pollutant} not found in data for prediction")
            
#             # Save predictions to a CSV file
#             self.save_predictions(predictions, data.index)
        
#         except Exception as e:
#             print(f"Error making predictions: {e}")
        
#         return predictions

#     def save_predictions(self, predictions, timestamps):
#         """Save predictions to a CSV file"""
#         try:
#             df_predictions = pd.DataFrame(predictions, index=timestamps)
#             prediction_file = os.path.join(PREDICTIONS_DIR, "predictions.csv")
            
#             # Append new predictions to the existing file, or create a new one
#             if os.path.exists(prediction_file):
#                 df_predictions.to_csv(prediction_file, mode='a', header=False)
#             else:
#                 df_predictions.to_csv(prediction_file, mode='w', header=True)
            
#             print(f"Predictions saved to {prediction_file}")
        
#         except Exception as e:
#             print(f"Error saving predictions: {e}")

#     def consume_data(self):
#         """Consume data from Kafka and make predictions"""
#         try:
#             # Subscribe to the topic
#             self.consumer.subscribe([self.topic])

#             while True:
#                 try:
#                     # Poll for new messages from Kafka
#                     msg = self.consumer.poll(timeout=1.0)

#                     if msg is None:
#                         # No new message
#                         continue
#                     if msg.error():
#                         # If there's an error
#                         if msg.error().code() == KafkaError._PARTITION_EOF:
#                             continue  # End of partition
#                         else:
#                             raise KafkaException(msg.error())

#                     # Process the message and make predictions
#                     data = json.loads(msg.value().decode('utf-8'))
#                     df = pd.DataFrame(data)  # Assuming data is a dictionary of lists
#                     df['Timestamp'] = pd.to_datetime(df['Timestamp'])
#                     df.set_index('Timestamp', inplace=True)

#                     # Make predictions
#                     self.make_predictions(df)

#                 except Exception as e:
#                     print(f"Error processing Kafka message: {e}")

#         except KeyboardInterrupt:
#             print("Terminating the consumer...")
#         finally:
#             # Close the consumer
#             self.consumer.close()

# def main():
#     """Main function to start the prediction service"""
#     kafka_config = {
#         'bootstrap.servers': 'localhost:9092',  # Kafka server(s)
#         'group.id': 'air_quality_consumer_group',  # Consumer group id
#         'auto.offset.reset': 'earliest'  # Start reading at the earliest offset
#     }
#     service = AirQualityPredictionService(kafka_config=kafka_config, topic='air_quality_data')
#     service.consume_data()

# if __name__ == "__main__":
#     main()

import json
import joblib
import pandas as pd
from confluent_kafka import Consumer
from statsmodels.tsa.arima.model import ARIMAResultsWrapper
import time

class AirQualityPredictor:
    def __init__(self):
        self.models = self.load_models()
        self.kafka_config = {
            'bootstrap.servers': 'localhost:9092',
            'group.id': 'air-quality-predicted',
            'auto.offset.reset': 'earliest'}
        
        self.consumer = Consumer(self.kafka_config)
        print(f"Connecting to Kafka at {self.kafka_config['bootstrap.servers']}")

        # Initialize predictions CSV
        self.predictions_file = 'predictions.csv'
        self.initialize_predictions_file()
        
        # Buffer for maintaining temporal features
        self.data_buffer = pd.DataFrame(columns=[
            'CO(GT)', 'NOx(GT)', 'C6H6(GT)', 
            'hour', 'day', 'month', 'day_of_week'])
        print("Temporal feature buffer initialized")

    def initialize_predictions_file(self):
        """Create CSV file with headers if it doesn't exist"""
        try:
            with open(self.predictions_file, 'x') as f:
                f.write('Timestamp,CO_actual,CO_rf,CO_arima,'
                        'NOx_actual,NOx_rf,NOx_arima,'
                        'C6H6_actual,C6H6_rf,C6H6_arima\n')
            print("Created new predictions file with headers")
                
        except FileExistsError:
            pass

    def load_models(self):
        """Load pre-trained models from disk"""
        models = {
            'random_forest': {},
            'arima': {}}
        
        pollutants = ['co', 'nox', 'c6h6']
        for poll in pollutants:
            try:
                models['random_forest'][poll] = joblib.load(f'models/model_{poll}.pkl')
                models['arima'][poll] = ARIMAResultsWrapper.load(f'models/arima_{poll}.pkl')
                print(f"Loading Random Forest model for {poll}...")
            except Exception as e:
                print(f"Error loading model for {poll}: {e}")
        
        return models

    def preprocess(self, message):
        """Preprocess incoming Kafka message"""
        try:
            data = json.loads(message.value())
            df = pd.DataFrame([data])
            
            # Extract and format timestamp
            # df = pd.DataFrame(message)
            timestamp = pd.to_datetime(data['Timestamp'])
            df['Timestamp'] = timestamp
            print(f"convereted to timestamp")
            # Convert numeric fields
            for col in ['CO(GT)', 'NOx(GT)', 'C6H6(GT)']:
                if col in data:
                    df[col] = pd.to_numeric(data[col], errors='coerce')
            
            # Temporal features
            df['hour'] = timestamp.hour
            df['day'] = timestamp.day
            df['month'] = timestamp.month
            df['day_of_week'] = timestamp.dayofweek
            
            # Update data buffer
            self.data_buffer = pd.concat([self.data_buffer, df]).tail(6)
            
            # Generate temporal features
            for poll in ['CO(GT)', 'NOx(GT)', 'C6H6(GT)']:
                if poll in df.columns:
                    df[f'{poll}_lag1'] = self.data_buffer[poll].shift(1).iloc[-1]
                    df[f'{poll}_rolling_mean3'] = self.data_buffer[poll].rolling(3).mean().iloc[-1]
                    df[f'{poll}_rolling_std3'] = self.data_buffer[poll].rolling(3).std().iloc[-1]
                print(f" - Generated features for {poll}")
            return df.fillna(method='ffill').fillna(0)
            
        except Exception as e:
            print(f"Preprocessing error: {e}")
            return None

    def predict(self, features):
        """Make predictions using loaded models"""
        predictions = {}
        
        # Random Forest predictions
        for poll, model in self.models['random_forest'].items():
            try:
                input_features = features[[
                    'hour', 'day', 'month', 'day_of_week',
                    f'{poll.upper()}(GT)_lag1',
                    f'{poll.upper()}(GT)_rolling_mean3',
                    f'{poll.upper()}(GT)_rolling_std3']]
                predictions[f'{poll}_rf'] = model.predict(input_features)[0]
                print("\nMaking predictions...")
            except KeyError:
                continue
                
        # ARIMA predictions
        for poll, model in self.models['arima'].items():
            try:
                arima_pred = model.forecast(steps=1).values[0]
                predictions[f'{poll}_arima'] = arima_pred
            except Exception as e:
                print(f"ARIMA prediction error: {e}")
        
        return predictions

    def save_predictions(self, timestamp, actuals, predictions):
        """Save predictions to CSV file"""
        try:
            row = {
                'Timestamp': timestamp,
                # CO predictions
                'CO_actual': actuals.get('CO(GT)', None),
                'CO_rf': predictions.get('co_rf', None),
                'CO_arima': predictions.get('co_arima', None),
                # NOx predictions
                'NOx_actual': actuals.get('NOx(GT)', None),
                'NOx_rf': predictions.get('nox_rf', None),
                'NOx_arima': predictions.get('nox_arima', None),
                # C6H6 predictions
                'C6H6_actual': actuals.get('C6H6(GT)', None),
                'C6H6_rf': predictions.get('c6h6_rf', None),
                'C6H6_arima': predictions.get('c6h6_arima', None)}
            
            df = pd.DataFrame([row])
            df.to_csv(self.predictions_file, mode='a', 
                     header=not pd.io.common.file_exists(self.predictions_file),
                     index=False)
            print("Predictions saved successfully")
        except Exception as e:
            print(f"Error saving predictions: {e}")

    def run(self):
        """Main prediction loop"""
        self.consumer.subscribe(['air_quality_data'])
        
        try:
            while True:
                msg = self.consumer.poll(1.0)
                
                if msg is None:
                    continue
                if msg.error():
                    print(f"Consumer error: {msg.error()}")
                    continue
                
                features = self.preprocess(msg)
                if features is not None:
                    # Extract actual values
                    actuals = {
                        'CO(GT)': features.get('CO(GT)', [None])[0],
                        'NOx(GT)': features.get('NOx(GT)', [None])[0],
                        'C6H6(GT)': features.get('C6H6(GT)', [None])[0]}
                    
                    # Make predictions
                    predictions = self.predict(features)
                    
                    # Save to CSV
                    self.save_predictions(
                        timestamp=features['Timestamp'].iloc[0],
                        actuals=actuals,
                        predictions=predictions)
                    
        except KeyboardInterrupt:
            print("\nStopping prediction service...")
        finally:
            self.consumer.close()
            print(f"Predictions saved to {self.predictions_file}")

if __name__ == "__main__":
    predictor = AirQualityPredictor()
    print("Starting air quality prediction service...")
    predictor.run()
