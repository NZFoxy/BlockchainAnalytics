import sqlite3
import os
import logging
from import_wallet import fetch_transactions  # Assuming this function fetches transactions

import rfc_main     # Assuming this function returns a classification

# Define the log file path
log_file = 'flagged_transactions.log'

# Check if the log file exists, and create it if it doesn't
if not os.path.exists(log_file):
    open(log_file, 'w').close()

# Configure logging
logging.basicConfig(filename=log_file, 
                    filemode='a',  # Use 'a' to append to the log if it already exists
                    format='%(asctime)s - %(message)s', 
                    level=logging.INFO)

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

def get_transactions_from_db(db_name='Database/transactions2.db'):
    """
    Fetches all transactions from the database.
    """
    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()
    
    # Fetch all transactions
    cursor.execute("SELECT * FROM transactions")
    transactions = cursor.fetchall()
    conn.close()
    
    return transactions

def classify_and_log_transactions(transactions):
    """
    Classifies the transactions using the Random Forest classifier and logs flagged transactions.
    """
    flagged_transactions = []
    
    for transaction in transactions:
        # Assuming the model function classify_transaction returns 'green', 'orange', or 'red'
        classification = rfc_main(transaction)
        
        if classification in ['orange', 'red']:
            flagged_transactions.append((transaction, classification))
            # Print the flagged transaction
            print(f"Flagged Transaction: {transaction}, Rating: {classification}")
            # Log the flagged transaction
            logging.info(f"Flagged Transaction: {transaction}, Rating: {classification}")
    
    return flagged_transactions

def main():
    # Take in the Polygon wallet address from the user
    wallet_address = input("Enter the Polygon wallet address: ")

    # Empty and recreate the transactions.db
    empty_and_recreate_transactions_db()

    # Populate the database with the wallet's transactions using import_requests.py
    fetch_transactions(wallet_address, 0, 9999999, 1000)  # Fetches and populates transactions2.db

    # Fetch the transactions from the database
    transactions = get_transactions_from_db()

    # Classify the transactions and log flagged ones
    classify_and_log_transactions(transactions)
    
    print("Process completed. Check 'flagged_transactions.log' for flagged transactions.")

if __name__ == "__main__":
    main()