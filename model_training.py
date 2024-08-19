import pandas as pd
import numpy as np
from sklearn.preprocessing import StandardScaler
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_squared_error, r2_score
from sklearn.model_selection import train_test_split
from fraud_scoring import calculate_fraud_score

def preprocess_data(df):
    # Convert data types
    df['value'] = df['value'].astype(float)
    df['gasPrice'] = df['gasPrice'].astype(float)
    df['gas'] = df['gas'].astype(float)
    df['timestamp'] = pd.to_datetime(df['timestamp'], unit='s').astype(int)
    
    # Calculate fraud score for each row and create a new column 'fraudulent_score'
    df['fraudulent_score'] = df.apply(calculate_fraud_score, axis=1)

    # Select features and label for model
    X = df[['value', 'gasPrice', 'gas', 'timestamp']]
    y = df['fraudulent_score']

    # Scale the features
    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X)

    return X_scaled, y, scaler

def train_model(X_train, y_train):
    regressor = RandomForestRegressor(n_estimators=100, random_state=42)
    regressor.fit(X_train, y_train)
    return regressor

def evaluate_model(regressor, X_test, y_test):
    y_pred = regressor.predict(X_test)
    print(f"Mean Squared Error: {mean_squared_error(y_test, y_pred)}")
    print(f"R^2 Score: {r2_score(y_test, y_pred)}")

def predict_transaction(transaction, scaler, regressor):
    # Scale the transaction data
    transaction_scaled = scaler.transform(transaction)  # Directly pass the DataFrame
    prediction = regressor.predict(transaction_scaled)
    return prediction[0]  # Assuming regressor.predict returns an array-like object

def predict_all_transactions(df, scaler, regressor):
    """Predict fraud scores for all transactions and return the predictions."""
    X = df[['value', 'gasPrice', 'gas', 'timestamp']]
    X_scaled = scaler.transform(X)
    predictions = regressor.predict(X_scaled)
    
    return predictions

