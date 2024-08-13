import pandas as pd
from sklearn.model_selection import train_test_split
import fetch_data
import ml_model

def main():
    # Fetch all transactions from the database
    data = fetch_data.fetch_data_from_sql('Database/transactions.db')

    # Preprocess the data
    X, y, scaler = ml_model.preprocess_data(data)

    # Split the data into training and testing sets
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.3, random_state=42)

    # Train the machine learning model
    regressor = ml_model.train_model(X_train, y_train)

    # Evaluate the model's performance
    ml_model.evaluate_model(regressor, X_test, y_test)

    # Predict fraud scores for all transactions
    predictions = ml_model.predict_all_transactions(data, scaler, regressor)
    
    # Initialize counters for each severity level
    green_count = 0
    orange_count = 0
    red_count = 0

    # Classify predictions based on the fraud score thresholds
    for pred in predictions:
        if pred <= 0.5:
            green_count += 1
        elif 0.5 <= pred <= 0.7:
            orange_count += 1
        elif 0.7 <= pred <= 1.0:
            red_count += 1

    print(f"Number of 'green' transactions: {green_count}")
    print(f"Number of 'orange' transactions: {orange_count}")
    print(f"Number of 'red' transactions: {red_count}")

    # Predict the fraud score for a new transaction
    new_transaction = pd.DataFrame([[15000, 0.0001, 21000, 1692799200]], 
                                    columns=['value', 'gasPrice', 'gas', 'timestamp'])  # Example: [value, gasPrice, gas, timestamp]
    fraud_score = ml_model.predict_transaction(new_transaction, scaler, regressor)
    
    # Determine severity level based on fraud score
    if fraud_score <= 0.5:
        severity = "green"
    elif 0.6 <= fraud_score <= 0.7:
        severity = "orange"
    elif 0.8 <= fraud_score <= 1.0:
        severity = "red"
    else:
        severity = "unclassified"  # In case the fraud score doesn't fit in any category

    print("Transaction severity:", severity)

if __name__ == "__main__":
    main()
