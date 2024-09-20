#main.py //Please run populate_trainig_data.py first before running this script//

import pandas as pd
import numpy as np
#%pip install seaborn ///// run this if you dont have seaborn yet
import seaborn as sns
import matplotlib.pyplot as plt
#%matplotlib inline
import transaction_data_pipeline
import populate_single_wallet
import sqlite3
import os

from populate_single_wallet import fetch_transactions

from sklearn import datasets, metrics
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier
#from sklearn.preprocessing import LabelEncoder
from sklearn.utils import Bunch

def main():
    print("Classifier script is running")
    
    #criterias for which the transaction is classified on  ########## use this for a)
    feature_columns = ['gasUsed', 'value', 'confirmations', 'nonce', 'txreceipt_status', 'gasPrice', 'cumulativeGasUsed', 'fromAddress', 'todAddress']

    ##### use this for b) and c)
    #feature_columns = ['gasUsed', 'value', 'confirmations', 'nonce', 'txreceipt_status', 'gasPrice', 'cumulativeGasUsed']

    #the "label"
    target_column = 'flag'


    ############################### a) Orignal (Using 136k transactions + rule based labelling)
    '''
    transactions_dataset = transaction_data_pipeline.create_dataset_from_df('Database/transactions.db',feature_columns,target_column)
    X = transactions_dataset.data
    y = transactions_dataset.target
    X_train, X_test, y_train, y_test = train_test_split(X, y, random_state = 1, test_size = 0.3, stratify = y)
    '''
    ###############################

    ############################## b) using Jacobs dataset
    '''
    transactions_dataframe = pd.read_csv('training.csv')

    X = transactions_dataframe[feature_columns]
    y = transactions_dataframe[target_column]

    target_names = ['green', 'orange', 'red']

    transactions_dataset = Bunch(data=X.values, target=y.values, feature_names=feature_columns, target_names=target_names)

    X_train, X_test, y_train, y_test = train_test_split(transactions_dataset.data, transactions_dataset.target, test_size=0.3, random_state=42)
    '''
    ########################################

    ############################# c) labelling Jacob's dataset and training on it
    
    transactions_dataframe = pd.read_csv('training1.csv')

    transactions_dataframe = transaction_data_pipeline.flag_transactions_csv(transactions_dataframe) #flag the "csv"

    X = transactions_dataframe[feature_columns]
    y = transactions_dataframe[target_column]

    target_names = ['green', 'orange', 'red']

    transactions_dataset = Bunch(data=X.values, target=y.values, feature_names=feature_columns, target_names=target_names)

    X_train, X_test, y_train, y_test = train_test_split(transactions_dataset.data, transactions_dataset.target, test_size=0.3, random_state=42)
    
    #############################


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

    # Empty and recreate the transactions2.db
    transaction_data_pipeline.empty_and_recreate_transactions_db()
    
    #ask for wallet and populate transactions2 database
    wallet_address = populate_single_wallet.main()
    
    # Connect to SQLite database
    conn = sqlite3.connect('Database/transactions2.db')

    # Build the query string
    if feature_columns:
        columns = ', '.join(feature_columns)
    
    #query = f"SELECT {columns} FROM Transactions"
    query = f"SELECT * FROM Transactions"

    # Execute query to fetch all data from the 'Transactions' table
    wallet_transactions_df = pd.read_sql_query(query, conn) #this is the dataframe

    ##########################
    # Load fraud wallets from the database
    fraud_wallets = transaction_data_pipeline.get_fraud_wallets('Database/fraud_wallets.db')

    # Create binary features for whether from_address or to_address are in the fraud_wallets set
    wallet_transactions_df['is_from_fraud_wallet'] = wallet_transactions_df['fromAddress'].apply(lambda x: 1 if x in fraud_wallets else 0)
    wallet_transactions_df['is_to_fraud_wallet'] = wallet_transactions_df['toAddress'].apply(lambda x: 1 if x in fraud_wallets else 0)
    ########################


    # Close the connection
    conn.close()
    # Debugging: Check the type of wallet_transactions_df
    print(f"Type of wallet_transactions_df: {type(wallet_transactions_df)}")

    actual_feature_columns = ['gasUsed', 'value', 'confirmations', 'nonce', 'txreceipt_status', 'gasPrice', 'cumulativeGasUsed', 'is_from_fraud_wallet', 'is_to_fraud_wallet']

    wallet_transactions_data = wallet_transactions_df[actual_feature_columns].values

    # Predict the labels for these transactions
    predicted_labels = clf.predict(wallet_transactions_data)
    #print(f"Predicted Labels: {predicted_labels}")

    predicted_labels_named = predicted_labels

    # Add the predicted labels to the DataFrame
    wallet_transactions_df['predicted_label'] = predicted_labels_named

    # Select only the columns you want to save
    result_df = wallet_transactions_df[['hash','fromAddress','predicted_label']]

    # Ensure the Database directory exists
    results_dir = 'Results'
    if not os.path.exists(results_dir):
     os.makedirs(results_dir)
    
     # Save the result to a CSV file in the Results folder
    result_df.to_csv('./Results/' + wallet_address + '.csv', index=False)

    print("Results saved to "+wallet_address+".csv")
    
if __name__ == "__main__":
    main()