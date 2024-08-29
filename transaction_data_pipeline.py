#Transaction_data_pipeline.py //This script is used to create a dataset from the transactions.db database 
#labels the transactions based on a rule based system//

import sqlite3
import pandas as pd
from sklearn.utils import Bunch

#2
def fetch_data_from_sql(database_path, query):
    # Connect to SQLite database
    conn = sqlite3.connect(database_path)
    
    # Execute query to fetch all data from the 'Transactions' table

    transactions_df = pd.read_sql_query(query, conn) #this is the dataframe

    #flagging the transactions with rule based labelling, then adding it as a new column
    transactions_df['flag'] = transactions_df.apply(label_transaction, axis=1)

    # Count the number of each flag
    flag_counts = transactions_df['flag'].value_counts()

    # Print out the number of red, green, and orange labels
    print(f"Number of 'red' flags: {flag_counts.get('red', 0)}")
    print(f"Number of 'green' flags: {flag_counts.get('green', 0)}")
    print(f"Number of 'orange' flags: {flag_counts.get('orange', 0)}")
    
    # Close the connection
    conn.close()

    return transactions_df

#4
def calculate_fraud_score(row):
    score = 0
    
    avg_gas = 21000  # Example average gas for a simple transaction
    if int(row['gasUsed']) > avg_gas * 5:
        score += 0.3
    
    if int(row['isError']) == 1 or int(row['txreceipt_status']) == 0:
        score += 0.1
    
    avg_value = 10 * (10**18)  # Example average value in Wei
    if int(row['value']) > avg_value * 100:
        score += 0.4
    
    if int(row['confirmations']) < 20000:
        score += 0.1
    
    if int(row['nonce']) > 100:  # Example threshold
        score += 0.1

    return min(score, 1)  # Cap the score at 1

#3
def label_transaction(row):

    #calculate fraud score here
    fraud_score = calculate_fraud_score(row)
           
    # Rule based labelling, this is to save time because manually labelling 136k transactions is pretty difficult
    if fraud_score <= 0.5:
        return 'green' 
    elif 0.5 <= fraud_score <= 0.7:
        return 'orange'
    elif 0.7 <= fraud_score <= 1.0:
        return 'red'


#1
def create_dataset_from_df(database_path, feature_columns,target_column):

    # Build the query string
    if feature_columns:
        columns = ', '.join(feature_columns)
        #if target_column:
        #   columns += f", {target_column}"
    else:
        columns = '*'
    
    query = f"SELECT {columns} FROM Transactions"   
    
    #getting the dataframe that contains only the feature and target columns
    transactions_df = fetch_data_from_sql(database_path, query)

    # Convert the DataFrame to a dataset
    
    # If there's a target column, separate it from the features
    if target_column and target_column in transactions_df.columns:
        target = transactions_df[target_column].values

        #dropping the column to separate it because we have separated is as the 'target' attribute
        transactions_df = transactions_df.drop(columns=[target_column])
    else:
        target = None

    data = transactions_df.values  # Convert DataFrame to NumPy array
    feature_names = transactions_df.columns.tolist()  # Extract column names as feature names

    target_names = ['green', 'orange', 'red']
    
    # Create a Bunch object to store the dataset
    dataset = Bunch(data=data, feature_names=feature_names, target=target, target_names=target_names, DESCR="Transactions Dataset")

    return dataset

#Empty and recreate the transactions2 database: This is a helper functiondef empty_and_recreate_transactions_db(db_name='Database/transactions2.db'):

def empty_and_recreate_transactions_db(db_name='Database/transactions2.db'):
    """
    Empties the transactions.db by dropping the transactions table and recreating it.
    """
    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()
    
    # Drop the table if it exists
    cursor.execute("DROP TABLE IF EXISTS transactions")
    print("Transactions table dropped successfully.")
    
    # Recreate the table with the appropriate structure
    cursor.execute("""
        CREATE TABLE transactions (
            hash TEXT PRIMARY KEY,
            nonce INTEGER,
            blockHash TEXT,
            blockNumber INTEGER,
            transactionIndex INTEGER,
            fromAddress TEXT,
            toAddress TEXT,
            value REAL,
            gas INTEGER,
            gasPrice INTEGER,
            isError INTEGER,
            txreceipt_status INTEGER,
            input TEXT,
            contractAddress TEXT,
            cumulativeGasUsed INTEGER,
            gasUsed INTEGER,
            confirmations INTEGER,
            timestamp TEXT
        )
    """)
    print("Transactions table recreated successfully.")
    conn.commit()

    cursor.close()
    conn.close()
  