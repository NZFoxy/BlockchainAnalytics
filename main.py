import pandas as pd
import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt
import transaction_data_pipeline
import populate_single_wallet
import sqlite3
import os

from populate_single_wallet import fetch_transactions
from sklearn import metrics
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier
from sklearn.utils import Bunch

def main():
    print("Classifier script is running")
    
    # Get user input for method selection
    print("Select the method you want to use:")
    print("a) Original dataset (Using 136k transactions + rule-based labeling)")
    print("b) Jacob's dataset")
    print("c) Labeling Jacob's dataset and training on it")
    
    method = input("Please enter 'a', 'b', or 'c': ").lower()

    if method not in ['a', 'b', 'c']:
        print("Invalid input. Please restart and enter a valid option.")
        return

    # Feature columns to use based on the method
    if method == 'a':
        feature_columns = ['gasUsed', 'value', 'confirmations', 'nonce', 'txreceipt_status', 'gasPrice', 'cumulativeGasUsed', 'fromAddress', 'toAddress']
    elif method == 'b' or method == 'c':
        feature_columns = ['gasUsed', 'value', 'confirmations', 'nonce', 'txreceipt_status', 'gasPrice', 'cumulativeGasUsed']

    # The target column (label)
    target_column = 'flag'
    
    if method == 'a':
        # Method a: Using the original dataset with 136k transactions and rule-based labeling
        transactions_dataset = transaction_data_pipeline.create_dataset_from_df('Database/transactions.db', feature_columns, target_column, method)
        X = transactions_dataset.data
        y = transactions_dataset.target
        X_train, X_test, y_train, y_test = train_test_split(X, y, random_state=1, test_size=0.3, stratify=y)
    
    elif method == 'b':
        # Method b: Using Jacob's dataset
        transactions_dataframe = pd.read_csv('training.csv')
        X = transactions_dataframe[feature_columns]
        y = transactions_dataframe[target_column]
        target_names = ['green', 'orange', 'red']
        transactions_dataset = Bunch(data=X.values, target=y.values, feature_names=feature_columns, target_names=target_names)
        X_train, X_test, y_train, y_test = train_test_split(transactions_dataset.data, transactions_dataset.target, test_size=0.3, random_state=42)
    
    elif method == 'c':
        # Method c: Labeling Jacob's dataset and training on it
        transactions_dataframe = pd.read_csv('training1.csv')
        transactions_dataframe = transaction_data_pipeline.flag_transactions_csv(transactions_dataframe,method)  # Flag the CSV
        X = transactions_dataframe[feature_columns]
        y = transactions_dataframe[target_column]
        target_names = ['green', 'orange', 'red']
        transactions_dataset = Bunch(data=X.values, target=y.values, feature_names=feature_columns, target_names=target_names)
        X_train, X_test, y_train, y_test = train_test_split(transactions_dataset.data, transactions_dataset.target, test_size=0.3, random_state=42)

    # Train RandomForest Classifier
    clf = RandomForestClassifier(n_estimators=100, random_state=42)
    clf.fit(X_train, y_train)
    print("RandomForestClassifier Training completed")

    # Evaluate model performance
    y_pred = clf.predict(X_test)
    print("Accuracy:", metrics.accuracy_score(y_test, y_pred))

    # Confusion matrix
    mat = metrics.confusion_matrix(y_test, y_pred)
    print(mat)
    sns.heatmap(mat, annot=True, fmt='d', cbar=False, xticklabels=transactions_dataset.target_names, yticklabels=transactions_dataset.target_names)
    plt.xlabel('Predicted')
    plt.ylabel('True')

    # Feature importance
    print("Feature importance %")
    print(clf.feature_importances_)

    # Empty and recreate the transactions2.db
    transaction_data_pipeline.empty_and_recreate_transactions_db()

    # Ask for wallet and populate transactions2 database
    wallet_address = populate_single_wallet.main()

    # Connect to SQLite database
    #conn = sqlite3.connect('Database/orange_wallet_db.db')
    conn = sqlite3.connect('Database/transactions2.db')

    # Build the query string
    if feature_columns:
        columns = ', '.join(feature_columns)
    
    query = f"SELECT * FROM Transactions"
    wallet_transactions_df = pd.read_sql_query(query, conn)

    # Load fraud wallets from the database and normalize them
    fraud_wallets = set(transaction_data_pipeline.normalize_address(addr) for addr in transaction_data_pipeline.get_fraud_wallets('Database/fraud_wallets.db'))

    # Normalize the addresses in the DataFrame and apply the cross-check
    wallet_transactions_df['is_from_fraud_wallet'] = wallet_transactions_df['fromAddress'].apply(
        lambda x: 1 if transaction_data_pipeline.normalize_address(x) in fraud_wallets else 0)
    wallet_transactions_df['is_to_fraud_wallet'] = wallet_transactions_df['toAddress'].apply(
        lambda x: 1 if transaction_data_pipeline.normalize_address(x) in fraud_wallets else 0)



    # Close the connection
    conn.close()

    if method == 'a':
        actual_feature_columns = ['gasUsed', 'value', 'confirmations', 'nonce', 'txreceipt_status', 'gasPrice', 'cumulativeGasUsed', 'is_from_fraud_wallet', 'is_to_fraud_wallet']
    elif method == 'b' or method == 'c':
        actual_feature_columns = ['gasUsed', 'value', 'confirmations', 'nonce', 'txreceipt_status', 'gasPrice', 'cumulativeGasUsed']
    
    wallet_transactions_data = wallet_transactions_df[actual_feature_columns].values

    # Predict the labels for these transactions
    predicted_labels = clf.predict(wallet_transactions_data)
    wallet_transactions_df['predicted_label'] = predicted_labels


    print("Random Forrest Model has finished predicting the labels of transactions of the given wallet")

    
    print("Now overriding predicted labels by crosschecking with fraud wallets database...")

    # Override predicted_label to 'red' if either is_from_fraud_wallet or is_to_fraud_wallet is 1
    wallet_transactions_df.loc[(wallet_transactions_df['is_from_fraud_wallet'] == 1) | 
                           (wallet_transactions_df['is_to_fraud_wallet'] == 1), 
                           'predicted_label'] = 'red'

    print("Override Complete")
    

    #Print results# Get the count of each label
    label_counts = wallet_transactions_df['predicted_label'].value_counts()

    # Print the counts
    total_transactions = len(wallet_transactions_df)
    num_red = label_counts.get('red', 0)
    num_orange = label_counts.get('orange', 0)
    num_green = label_counts.get('green', 0)

    print(f"Total transactions pulled: {total_transactions}")
    print(f"Red transactions: {num_red}")
    print(f"Orange transactions: {num_orange}")
    print(f"Green transactions: {num_green}")

    # Save the results to a CSV
    result_df = wallet_transactions_df[['hash', 'fromAddress', 'predicted_label']]
    results_dir = 'Results'
    if not os.path.exists(results_dir):
        os.makedirs(results_dir)

    result_df.to_csv(f'./Results/{wallet_address}.csv', index=False)
    print(f"Results saved to {wallet_address}.csv")

if __name__ == "__main__":
    main()
