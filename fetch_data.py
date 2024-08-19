"""
fetch_data.py

This module provides functions to fetch data from an SQLite database and label transactions
based on predefined rules. The labeled data can be used for further analysis or machine learning
purposes. It also includes functionality to count and report the number of each type of label.
"""

import sqlite3
import pandas as pd
from sklearn.utils import Bunch
# import logging

# Set up logging (commented out for now)
# logging.basicConfig(level=logging.INFO)

def fetch_data_from_sql(database_path: str, query: str) -> pd.DataFrame:
    """
    Fetch data from an SQLite database and apply rule-based labeling to transactions.

    Parameters:
    - database_path (str): The file path to the SQLite database.
    - query (str): The SQL query to execute.

    Returns:
    - pd.DataFrame: A DataFrame containing the queried data with an additional 'flag' column for labels.
    """
    try:
        # Connect to SQLite database
        conn = sqlite3.connect(database_path)
        
        # Execute query to fetch data
        transactions_df = pd.read_sql_query(query, conn)
        
        # Flag transactions with rule-based labeling
        transactions_df['flag'] = transactions_df.apply(label_transaction, axis=1)
        
        # Count the number of each flag (Uncomment logging lines to switch to logging)
        print(f"Number of 'red' flags: {flag_counts.get('red', 0)}")
        print(f"Number of 'green' flags: {flag_counts.get('green', 0)}")
        print(f"Number of 'orange' flags: {flag_counts.get('orange', 0)}")
        # logging.info(f"Number of 'red' flags: {flag_counts.get('red', 0)}")
        # logging.info(f"Number of 'green' flags: {flag_counts.get('green', 0)}")
        # logging.info(f"Number of 'orange' flags: {flag_counts.get('orange', 0)}")

        return transactions_df
    
    except sqlite3.Error as e:
        print(f"Database error: {e}")
        # logging.error(f"Database error: {e}")
        return pd.DataFrame()  # Return an empty DataFrame on error
    
    finally:
        conn.close()

def label_transaction(row: pd.Series) -> str:
    """
    Label a transaction based on predefined rules.

    Parameters:
    - row (pd.Series): A row of transaction data.

    Returns:
    - str: The label ('red', 'green', 'orange') assigned to the transaction.
    """
    # Placeholder function: implement the actual labeling logic
    return "green"  # Example: default to green
