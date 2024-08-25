import sqlite3
import os
import logging
from import_requests import fetch_tx_by_address  # Assuming this function fetches transactions
from rfc_main import rfc_main       # Assuming this function returns a classification

# Define the log file path
log_file = 'flagged_transactions.log'

# Check if the log file exists, and create it if it doesn't
if not os.path.exists(log_file):
    open(log_file, 'w').close()
    
 # Fetch API key from environment variable
api_key = os.getenv('POLYGONSCAN_API_KEY')
if not api_key:
        raise ValueError("API Key is not set in environment variables")

# Configure logging
logging.basicConfig(filename=log_file, 
                    filemode='a',  # Use 'a' to append to the log if it already exists
                    format='%(asctime)s - %(message)s', 
                    level=logging.INFO)

def empty_and_recreate_transactions_db(db_location='Database/transactions.db'):
    """
    Empties the transactions.db by dropping the transactions table and recreating it.
    """
    conn = sqlite3.connect('db_location')
    cursor = conn.cursor()
    
    # Drop the table if it exists
    cursor.execute("DROP TABLE IF EXISTS transactions")
    conn.commit()
    
    # Recreate the table with the appropriate structure
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS transactions (
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
    conn.commit()
    conn.close()

def get_transactions_from_db(db_name='transactions.db'):
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

def main():
    # Take in the Polygon wallet address from the user
    wallet_address = input("Enter the Polygon wallet address: ")

    # Empty and recreate the transactions.db
    empty_and_recreate_transactions_db()

    # Populate the database with the wallet's transactions using import_requests.py
    fetch_tx_by_address(wallet_address, 0, 9999999, 1000, depth=0, max_depth=1)  # Fetches and populates transactions.db

    # Fetch the transactions from the database
    transactions = get_transactions_from_db()

    # Classify the transactions and log flagged ones
    rfc_main()
    
    print("Process completed. Check 'flagged_transactions.log' for flagged transactions.")

if __name__ == "__main__":
    main()
