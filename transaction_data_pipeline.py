#Transaction_data_pipeline.py //This script is used to create a dataset from the transactions.db database 
#labels the transactions based on a rule based system//

import sqlite3
import pandas as pd
from sklearn.utils import Bunch

#1
def create_dataset_from_df(database_path, feature_columns,target_column, method):

    # Build the query string
    if feature_columns:
        columns = ', '.join(feature_columns)
        #if target_column:
        #   columns += f", {target_column}"
    else:
        columns = '*'
    
    query = f"SELECT {columns} FROM Transactions"   
    
    method = method.lower()
    
    #getting the dataframe that contains only the feature and target columns 
    #calls #2
    transactions_df = fetch_data_from_sql(database_path, query, method)

    # Convert the DataFrame to a dataset
    
    # If there's a target column, separate it from the features
    if target_column and target_column in transactions_df.columns:
        target = transactions_df[target_column].values

        #dropping the column to separate it because we have separated is as the 'target' attribute
        transactions_df = transactions_df.drop(columns=[target_column])
    else:
        target = None

    #drop the fromAddress and toAddress columns as they are strings, use is_from_fraud_wallet and is_to_fraud_wallet instead
    transactions_df = transactions_df.drop(columns=['fromAddress'])
    transactions_df = transactions_df.drop(columns=['toAddress'])

    data = transactions_df.values  # Convert DataFrame to NumPy array
    feature_names = transactions_df.columns.tolist()  # Extract column names as feature names

    target_names = ['green', 'orange', 'red']
    
    # Create a Bunch object to store the dataset
    dataset = Bunch(data=data, feature_names=feature_names, target=target, target_names=target_names, DESCR="Transactions Dataset")

    return dataset

#2
def fetch_data_from_sql(database_path, query, method):
    # Connect to SQLite database
    conn = sqlite3.connect(database_path)
    
    # Execute query to fetch all data from the 'Transactions' table
    transactions_df = pd.read_sql_query(query, conn)

    
    if method == 'a':
        # Load fraud wallets from the database and normalize them
        fraud_wallets = set(normalize_address(addr) for addr in get_fraud_wallets('Database/fraud_wallets.db'))
        #print(f"Fraud wallets (normalized): {fraud_wallets}")

        # Normalize the addresses in the DataFrame and apply the cross-check
        transactions_df['is_from_fraud_wallet'] = transactions_df['fromAddress'].apply(
            lambda x: 1 if normalize_address(x) in fraud_wallets else 0)
        transactions_df['is_to_fraud_wallet'] = transactions_df['toAddress'].apply(
            lambda x: 1 if normalize_address(x) in fraud_wallets else 0)
    

    # Flag the transactions with rule-based labelling
    transactions_df['flag'] = transactions_df.apply(lambda row: label_transaction(row, method), axis=1)

    # Count the number of each flag
    flag_counts = transactions_df['flag'].value_counts()

    # Print out the number of red, green, and orange labels
    print(f"Number of 'red' flags: {flag_counts.get('red', 0)}")
    print(f"Number of 'green' flags: {flag_counts.get('green', 0)}")
    print(f"Number of 'orange' flags: {flag_counts.get('orange', 0)}")
    
    # Close the connection
    conn.close()

    return transactions_df

def flag_transactions_csv(transactions_df, method):
    # Flagging the transactions with rule-based labelling
    method = method.lower()
    transactions_df['flag'] = transactions_df.apply(lambda row: label_transaction(row, method), axis=1)

    # Count the number of each flag
    flag_counts = transactions_df['flag'].value_counts()

    # Print out the number of red, green, and orange labels
    print(f"Number of 'red' flags: {flag_counts.get('red', 0)}")
    print(f"Number of 'green' flags: {flag_counts.get('green', 0)}")
    print(f"Number of 'orange' flags: {flag_counts.get('orange', 0)}")

    return transactions_df

#3
def label_transaction(row, method):
    # Calculate fraud score here based on the method
    fraud_score = calculate_fraud_score(row, method)
           
    # Rule-based labelling
    if fraud_score < 0.5:
        return 'green' 
    elif 0.5 <= fraud_score < 0.7:
        return 'orange'
    elif 0.7 <= fraud_score <= 1.0:
        return 'red'

#4
def calculate_fraud_score(row, method):
    score = 0

    '''
    # Average values from the database
    avg_gas = 362103.233422042
    avg_value = 1.88398547269491e+19
    avg_gas_price = 83268624715
    avg_cumulative_gas = 9187077.1295445

    # Score calculation based on certain thresholds
    if int(row['gasUsed']) > avg_gas * 5:
        score += 0.4
    if int(row['value']) > avg_value * 100:
        score += 0.4
    if int(row['confirmations']) < 10000:
        score += 0.2
    if int(row['nonce']) > 100:
        score += 0.1
    if int(row['gasPrice']) > avg_gas_price * 10:
        score += 0.3
    if int(row['cumulativeGasUsed']) > avg_cumulative_gas * 10:
        score += 0.2
    '''

    mean_gasUsed = 487483.700398221
    mean_value = 1.25353784685658e+21
    mean_confirmations = 40566927.1218456
    mean_nonce = 5386.11399239341
    mean_gasPrice = 45862339962.2391
    mean_cumulativeGasUsed = 8703764.19729006
    
    stdev_gasUsed = 1229467.56529239
    stdev_value = 6.61993367968414e+22
    stdev_confirmations = 8770610.74755724
    stdev_nonce = 62980.3777420489
    stdev_gasPrice = 338528269404.209 
    stdev_cumulativeGasUsed = 6944482.88998016

    if mean_gasUsed + (stdev_gasUsed) <= int(row['gasUsed']):
        score += 0.4
    if mean_value + (stdev_value )  <= int(row['value']):
        score += 0.4
    if mean_confirmations + (stdev_confirmations)  <= int(row['confirmations']):
        score += 0.2
    if mean_nonce + (stdev_nonce ) <= int(row['nonce']):
        score += 0.1
    if mean_gasPrice + (stdev_gasPrice ) <= int(row['gasPrice']):
        score += 0.3
    if mean_cumulativeGasUsed + (stdev_cumulativeGasUsed ) <= int(row['cumulativeGasUsed']):
        score += 0.2

    
    # Check fraud wallets only for method 'a'
    if method == 'a':
        is_from_fraud_wallet = row['is_from_fraud_wallet']
        is_to_fraud_wallet = row['is_to_fraud_wallet']
        
        # If either address is in the fraud wallets list, increase the score by 1
        if is_from_fraud_wallet == 1 or is_to_fraud_wallet == 1:
            score += 1
    

    return min(score, 1)  # Cap the score at 1

    # Connect to SQLite database and retrieve fraudulent wallets
def get_fraud_wallets(database_path):
    conn = sqlite3.connect(database_path)
    query = "SELECT address FROM fraud_wallets"  # Assuming the fraud wallets are stored in a column named 'wallet_address'
    fraud_wallets_df = pd.read_sql_query(query, conn)
    conn.close()

    # Convert the fraud_wallets_df to a set for faster lookup
    fraud_wallets = set(fraud_wallets_df['address'])
    return fraud_wallets

    # Normalize function to ensure consistent address formatting
def normalize_address(address):
    if isinstance(address, str):
        return address.lower().strip()  # Convert to lowercase and strip whitespace
    return address

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
  