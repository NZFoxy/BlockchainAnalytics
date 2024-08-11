from sklearn.model_selection import train_test_split
import fetch_data
import ml_model

DATABASE_PATH = 'Database/transactions.db'

def main():
    # Fetch all transactions from the database
    data = fetch_data.fetch_data_from_sql(DATABASE_PATH)

    # Preprocess the data
    X, y, scaler = ml_model.preprocess_data(data)

    # Split the data into training and testing sets
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.3, random_state=42)

    # Train the machine learning model
    clf = ml_model.train_model(X_train, y_train)

    # Evaluate the model's performance!
    ml_model.evaluate_model(clf, X_test, y_test)

    # Classify all transactions and count the number of each type
    red_count, green_count = ml_model.classify_all_transactions(data, scaler, clf)
    print(f"Number of 'red' transactions: {red_count}")
    print(f"Number of 'green' transactions: {green_count}")

    # Classify a new transaction as an example
    new_transaction = [15000, 0.0001, 21000, 1692799200]  # Example: [value, gasPrice, gas, timestamp]
    severity = ml_model.classify_transaction(new_transaction, scaler, clf)
    print("Transaction severity:", severity)

if __name__ == "__main__":
    main()
