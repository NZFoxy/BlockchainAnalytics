import pandas as pd
import numpy as np
#%pip install seaborn ///// run this if you dont have seaborn yet
import seaborn as sns
import matplotlib.pyplot as plt
#%matplotlib inline
import fetch_data
import import_wallet
import sqlite3

import daniel
from import_wallet import fetch_transactions
from import_requests import fetch_tx_by_address

from sklearn import datasets, metrics
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier




def main():
    print("Classifier script is running")
    
    #criterias for which the transaction is classified on
    feature_columns = ['gasUsed', 'isError', 'value', 'confirmations', 'nonce', 'txreceipt_status']

    #the "label"
    target_column = 'flag'

    transactions_dataset = fetch_data.create_dataset_from_df('Database/transactions.db',feature_columns,target_column)

    #print(transactions_dataset.DESCR)

    #print(transactions_dataset.data)

    #print(transactions_dataset.target)

    #print(transactions_dataset.feature_names)


    #print(transactions_dataset.target_names)

    X = transactions_dataset.data
    y = transactions_dataset.target

    X_train, X_test, y_train, y_test = train_test_split(X, y, random_state = 1, test_size = 0.3, stratify = y)

    clf = RandomForestClassifier(n_estimators = 100, random_state = 42)

    clf.fit(X_train, y_train)

    print("RandomForestClassifier Training completed")

    # Evaluate model performance
    y_pred = clf.predict(X_test)
    print(metrics.accuracy_score(y_test, y_pred))

    mat = metrics.confusion_matrix(y_test, y_pred)

    print(mat)

    sns.heatmap(mat, annot = True, fmt = 'd', cbar = False, xticklabels = transactions_dataset.target_names, yticklabels = transactions_dataset.target_names)
    plt.xlabel('Predicted')
    plt.ylabel('True')

    print("Feature importance %")
    print(clf.feature_importances_)

    # Take in the Polygon wallet address from the user
    #wallet_address = input("Enter the Polygon wallet address: ")

    # Empty and recreate the transactions2.db
    daniel.empty_and_recreate_transactions_db()
    
    #ask for wallet and populate transactions2 database
    import_wallet.main()

    # Connect to SQLite database
    conn = sqlite3.connect('Database/transactions2.db')

    # Build the query string
    if feature_columns:
        columns = ', '.join(feature_columns)
    
    #query = f"SELECT {columns} FROM Transactions"
    query = f"SELECT * FROM Transactions"

    # Execute query to fetch all data from the 'Transactions' table
    wallet_transactions_df = pd.read_sql_query(query, conn) #this is the dataframe
    # Close the connection
    conn.close()
    # Debugging: Check the type of wallet_transactions_df
    print(f"Type of wallet_transactions_df: {type(wallet_transactions_df)}")

    wallet_transactions_data = wallet_transactions_df[feature_columns].values

    # Predict the labels for these transactions
    predicted_labels = clf.predict(wallet_transactions_data)
    #print(f"Predicted Labels: {predicted_labels}")

    predicted_labels_named = predicted_labels

    ''''
    # Map the predicted numeric labels back to their names
    label_map = {0: 'green', 1: 'orange', 2: 'red'}
    predicted_labels_named = [label_map[label] for label in predicted_labels]
    '''

    # Add the predicted labels to the DataFrame
    wallet_transactions_df['predicted_label'] = predicted_labels_named

    
    # Select only the columns you want to save
    result_df = wallet_transactions_df[['hash', 'predicted_label']]

    # Save the result to a CSV file
    result_df.to_csv('wallet_transactions_with_labels.csv', index=False)

    print("Results saved to wallet_transactions_with_labels.csv")
    

    '''
    # Print the resulting transactions with their predicted labels
    print(wallet_transactions_df[['hash', 'predicted_label']])
    '''

if __name__ == "__main__":
    main()