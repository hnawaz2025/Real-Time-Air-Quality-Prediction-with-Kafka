# # import pandas as pd
# # import numpy as np
# # import joblib
# # from sklearn.preprocessing import StandardScaler
# # from sklearn.ensemble import RandomForestRegressor
# # from statsmodels.tsa.arima.model import ARIMA
# # from sklearn.metrics import mean_absolute_error, mean_squared_error
# # from sklearn.model_selection import train_test_split

# # # --- Load Preprocessed Data ---
# # data = pd.read_csv('air_quality_streamed.csv', parse_dates=['Timestamp'], index_col='Timestamp')

# # # --- Feature Engineering ---
# # data['hour'] = data.index.hour
# # data['day'] = data.index.day
# # data['month'] = data.index.month

# # # Lag features and rolling stats
# # for pollutant in ['CO(GT)', 'NOx(GT)', 'C6H6(GT)']:
# #     data[f'{pollutant}_lag1'] = data[pollutant].shift(1)
# #     data[f'{pollutant}_rolling_mean3'] = data[pollutant].rolling(window=3).mean()
# #     data[f'{pollutant}_rolling_std3'] = data[pollutant].rolling(window=3).std()

# # data.dropna(inplace=True)  # Drop rows with NaNs created by lag/rolling

# # # --- Model Training Loop ---
# # targets = ['CO(GT)', 'NOx(GT)', 'C6H6(GT)']
# # results = {}
# # results_arima = {}

# # for target in targets:
# #     print(f"Training model for: {target}")
    
# #     # Define features
# #     feature_cols = ['hour', 'day', 'month',
# #                     f'{target}_lag1', f'{target}_rolling_mean3', f'{target}_rolling_std3']
    
# #     X = data[feature_cols]
# #     y = data[target]
    
# #     # Chronological Train-Test Split
# #     train_size = int(len(X) * 0.8)
# #     X_train, X_test = X.iloc[:train_size], X.iloc[train_size:]
# #     y_train, y_test = y.iloc[:train_size], y.iloc[train_size:]
    
# #     # --- Scaling the Features ---
# #     scaler = StandardScaler()
# #     X_train_scaled = scaler.fit_transform(X_train)  # Fit on training data and transform
# #     X_test_scaled = scaler.transform(X_test)  # Only transform test data
    
# #     # Train RandomForest model
# #     model = RandomForestRegressor(n_estimators=100, random_state=42)
# #     model.fit(X_train_scaled, y_train)

# #     # Predictions
# #     y_pred = model.predict(X_test_scaled)

# #     # Evaluation
# #     mae = mean_absolute_error(y_test, y_pred)
# #     rmse = mean_squared_error(y_test, y_pred, squared=False)

# #     results[target] = {'MAE': mae, 'RMSE': rmse}
# #     print(f"{target} - MAE: {mae:.3f}, RMSE: {rmse:.3f}")

# #     # Save model
# #     joblib.dump(model, f'models/model_{target.replace("(GT)", "").lower()}.pkl')
    
# #     # Save scaler for future use
# #     joblib.dump(scaler, f'models/scaler_{target.replace("(GT)", "").lower()}.pkl')

# #     # --- ARIMA model [BONUS] ---
# #     train_data = data.iloc[:train_size]
# #     test_data = data.iloc[train_size:]

# #     arima_model = ARIMA(train_data[target], order=(5, 1, 0))  
# #     arima_fit = arima_model.fit()
# #     arima_predictions = arima_fit.forecast(steps=len(test_data))

# #     mae_arima = mean_absolute_error(y_test, arima_predictions)
# #     rmse_arima = mean_squared_error(y_test, arima_predictions)
# #     results_arima[target] = {'MAE': mae_arima, 'RMSE': rmse_arima}

# #     print("ARIMA MAE:", mean_absolute_error(y_test, arima_predictions))
# #     print("ARIMA RMSE:", np.sqrt(mean_squared_error(y_test, arima_predictions)))

# # # --- Save metrics ---
# # results_random_forest = pd.DataFrame(results).T
# # results_ARIMA = pd.DataFrame(results_arima).T

# # results_random_forest.to_csv('models/model_metrics.csv')
# # print("Training complete. Models and metrics saved.")

import joblib
import numpy as np
import pandas as pd
import json
import os
import argparse
from statsmodels.tsa.arima.model import ARIMA
from sklearn.ensemble import RandomForestRegressor
from sklearn.model_selection import train_test_split, TimeSeriesSplit
from sklearn.metrics import mean_absolute_error, mean_squared_error


MODELS_DIR = 'models/'  
DATA_PATH = 'air_quality_streamed.csv'
POLLUTANTS = ['CO(GT)', 'NOx(GT)', 'C6H6(GT)']

class AirQualityModelTrainer:
    def __init__(self, data_path=DATA_PATH):
        """Initialize the model trainer with data path"""
        self.data_path = data_path
        self.data = None
        self.rf_models = {}
        self.arima_models = {}
        
        # Create models directory if it doesn't exist
        if not os.path.exists(MODELS_DIR):
            os.makedirs(MODELS_DIR)
            print(f"Created models directory at {MODELS_DIR}")
            
        # Load data
        self.load_data()
        
    def load_data(self):
        """Load air quality dataset"""
        try:
            self.data = pd.read_csv(self.data_path, parse_dates=['Timestamp'])
            self.data.set_index('Timestamp', inplace=True)
            self.data.sort_index(inplace=True)
            
            # Check for presence of target pollutants
            for pollutant in POLLUTANTS:
                if pollutant in self.data.columns:
                    print(f"Found target pollutant: {pollutant}")
                else:
                    print(f"Warning: Target pollutant {pollutant} not found in dataset")
            
        except Exception as e:
            print(f"Error loading data: {e}")
            raise
            
    def preprocess_data(self):
        """Preprocess data for modeling"""
        try:
            # Handle missing values
            self.data = self.data.fillna(method='ffill')  # Forward fill
            self.data = self.data.fillna(method='bfill')  # Backward fill remaining NAs
            
            # Create time features
            self.data['hour'] = self.data.index.hour
            self.data['day'] = self.data.index.day
            self.data['month'] = self.data.index.month
            self.data['day_of_week'] = self.data.index.dayofweek
                
            # Drop any remaining rows with NAs
            initial_rows = len(self.data)
            self.data = self.data.dropna()
            if initial_rows > len(self.data):
                print(f"Dropped {initial_rows - len(self.data)} rows with missing values")
                
            print("Data preprocessing complete")
                
        except Exception as e:
            print(f"Error preprocessing data: {e}")
            raise
            
    def create_features(self, pollutant):
        """Create features for a specific pollutant"""
        try:
            # Create a copy of the dataset for this pollutant
            df = self.data.copy()
            
            # Create lag features
            df[f'{pollutant}_lag1'] = df[pollutant].shift(1)
            df[f'{pollutant}_lag2'] = df[pollutant].shift(2)
            df[f'{pollutant}_lag3'] = df[pollutant].shift(3)
            
            # Create rolling statistics
            df[f'{pollutant}_rolling_mean3'] = df[pollutant].rolling(window=3).mean()
            df[f'{pollutant}_rolling_std3'] = df[pollutant].rolling(window=3).std()
            df[f'{pollutant}_rolling_mean6'] = df[pollutant].rolling(window=6).mean()
            
            # Add periodic features for ARIMA residuals
            df[f'{pollutant}_diff1'] = df[pollutant].diff(1)
            
            # Drop rows with NaN from feature creation
            df = df.dropna()
            
            # Define features
            feature_cols = [
                'hour', 'day', 'month', 'day_of_week',
                f'{pollutant}_lag1', f'{pollutant}_lag2', f'{pollutant}_lag3',
                f'{pollutant}_rolling_mean3', f'{pollutant}_rolling_std3', 
                f'{pollutant}_rolling_mean6'
            ]
            
            # Add other pollutants as features if available
            for other_pollutant in POLLUTANTS:
                if other_pollutant != pollutant and other_pollutant in df.columns:
                    feature_cols.append(other_pollutant)
            
            # Create X and y
            X = df[feature_cols]
            y = df[pollutant]
            
            return X, y, df
            
        except Exception as e:
            print(f"Error creating features for {pollutant}: {e}")
            raise
            
    def train_random_forest(self, pollutant):
        """Train a Random Forest model for a specific pollutant"""
        try:
            print(f"\nTraining Random Forest model for {pollutant}...")
            
            # Create features
            X, y, _ = self.create_features(pollutant)
            
            # Split data
            X_train, X_test, y_train, y_test = train_test_split(
                X, y, test_size=0.2, shuffle=False, random_state=5)
            
            # Define and train model
            rf_model = RandomForestRegressor(
                n_estimators=100,
                max_depth=10,
                min_samples_split=5,
                min_samples_leaf=2,
                random_state=5,
                n_jobs=-1)
            
            rf_model.fit(X_train, y_train)
            
            # Evaluate model
            y_pred = rf_model.predict(X_test)
            mae = mean_absolute_error(y_test, y_pred)
            rmse = np.sqrt(mean_squared_error(y_test, y_pred))

            print(f"Random Forest Model Evaluation for {pollutant}:")
            print(f"  - MAE: {mae:.4f}")
            print(f"  - RMSE: {rmse:.4f}")
              
            # Store model
            self.rf_models[pollutant] = rf_model
            
            # Save model
            model_name = pollutant.replace('(GT)', '').lower()
            model_path = f"{MODELS_DIR}model_{model_name}.pkl"
            joblib.dump(rf_model, model_path)
            print(f"Random Forest model saved to {model_path}")
            
            return rf_model, (mae, rmse)
            
        except Exception as e:
            print(f"Error training Random Forest model for {pollutant}: {e}")
            raise
            
    def train_arima(self, pollutant, order=(1, 1, 1)):
        """Train an ARIMA model for a specific pollutant"""
        try:
            print(f"\nTraining ARIMA model for {pollutant}...")
            
            # Get time series for this pollutant
            series = self.data[pollutant].copy()
            
            # Split data
            train_size = int(len(series) * 0.8)
            train, test = series[:train_size], series[train_size:]
            
            # Fit ARIMA model
            model = ARIMA(train, order=order)
            fitted_model = model.fit()
            
            # Make predictions
            predictions = fitted_model.forecast(steps=len(test))
            
            # Evaluate
            mae = mean_absolute_error(test, predictions)
            rmse = np.sqrt(mean_squared_error(test, predictions))
            
            print(f"ARIMA Model Evaluation for {pollutant}:")
            print(f"  - Order: {order}")
            print(f"  - MAE: {mae:.4f}")
            print(f"  - RMSE: {rmse:.4f}")
            
            # Store model
            self.arima_models[pollutant] = fitted_model
            
            # Save model
            model_name = pollutant.replace('(GT)', '').lower()
            model_path = f"{MODELS_DIR}arima_{model_name}.pkl"
            fitted_model.save(model_path)
            print(f"ARIMA model saved to {model_path}")
            
            return fitted_model, (mae, rmse)
            
        except Exception as e:
            print(f"Error training ARIMA model for {pollutant}: {e}")
              
    def train_all_models(self):
        """Train all models for all pollutants"""
        # Preprocess data first
        self.preprocess_data()
        
        # Results storage
        results = {'random_forest': {},'arima': {}}
        
        # Train models for each pollutant
        for pollutant in POLLUTANTS:
            if pollutant in self.data.columns:
                print(f"\n{'='*50}")
                print(f"Training models for {pollutant}")
                print(f"{'='*50}")
                
                # Train Random Forest
                _, rf_metrics = self.train_random_forest(pollutant)
                results['random_forest'][pollutant] = rf_metrics
                
                # Train ARIMA
                _, arima_metrics = self.train_arima(pollutant)
                results['arima'][pollutant] = arima_metrics
            else:
                print(f"Skipping {pollutant} - not found in dataset")
                
        # Print summary of results
        print("\n\nTraining Results Summary:")
        print("========================")
        
        print("\nRandom Forest Models:")
        for pollutant, metrics in results['random_forest'].items():
            print(f"  - {pollutant}: MAE={metrics[0]:.4f}, RMSE={metrics[1]:.4f}")
            
        print("\nARIMA Models:")
        for pollutant, metrics in results['arima'].items():
            if not np.isnan(metrics[0]):
                print(f"  - {pollutant}: MAE={metrics[0]:.4f}, RMSE={metrics[1]:.4f}")
            else:
                print(f"  - {pollutant}: Training failed")
                
        return results

def main():
    """Main function"""
    parser = argparse.ArgumentParser(description='Train air quality prediction models')
    parser.add_argument('--data', type=str, default=DATA_PATH, 
                        help='Path to air quality dataset CSV file')
    args = parser.parse_args()
    
    print("Air Quality Model Trainer")
    print("========================")
    print(f"Data path: {args.data}")
    
    # Create and run trainer
    trainer = AirQualityModelTrainer(data_path=args.data)
    results = trainer.train_all_models()
    
    print("\nTraining complete. Models saved to:", MODELS_DIR)

if __name__ == "__main__":
    main()

