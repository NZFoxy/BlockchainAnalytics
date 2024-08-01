import os
import requests
import json
import time
from urllib.parse import urlencode, urlunparse
import pyodbc
import pandas as pd
from decimal import Decimal
import logging

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Ensure API key is set
api_key = os.getenv('POLYGONSCAN_API_KEY')
if not api_key:
    raise ValueError("API Key is not set in environment variables")

# Azure SQL Database connection details
server = 'autblockchain.database.windows.net'
database = 'TransactionsAll'
username = 'CloudSAcab2136b'
password = 'Bottles1!'  # Ask Daniel
driver = '{ODBC Driver 18 for SQL Server}'

# Ensure all database connection details are set
if not all([server, database, username, password]):
    raise ValueError("Azure SQL Database connection details are not fully set in environment variables")

# Directory for errors
error_directory = 'errors'
if not os.path.exists(error_directory):
    os.makedirs(error_directory)

# Set to track processed wallets
processed_wallets = set()

def log_error(wallet_address, error_message):
    """Log the wallet address and error message to a JSON file."""
    error_log = {
        'wallet_address': wallet_address,
        'error_message': error_message,
        'timestamp': time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime())  # Add timestamp to log
    }
    error_filename = f'{error_directory}/errors.json'
    if os.path.exists(error_filename):
        with open(error_filename, 'r') as file:
            errors = json.load(file)
    else:
        errors = []
    errors.append(error_log)
    with open(error_filename, 'w') as file:
        json.dump(errors, file, indent=4)
    logging.error(f"Error logged for address {wallet_address}")

def get_starting_block(wallet_address):
    """Fetch the starting block number for a specific address from Polygonscan."""
    query_params = {
        'module': 'account',
        'action': 'txlist',
        'address': wallet_address,
        'startblock': 0,
        'endblock': 99999999,
        'sort': 'asc',
        'apikey': api_key,
        'page': 1,
        'offset': 1  # Limit to first transaction to get starting block
    }
    url_parts = ('https', 'api.polygonscan.com', '/api', '', urlencode(query_params), '')
    url = urlunparse(url_parts)
    try:
        response = requests.get(url)
        response.raise_for_status()
        tx_data = response.json()
        if 'result' in tx_data and isinstance(tx_data['result'], list) and len(tx_data['result']) > 0:
            return int(tx_data['result'][0]['blockNumber'])  # Return block number of first transaction
        else:
            logging.warning("No transactions found for this address.")
            return None
    except requests.RequestException as e:
        log_error(wallet_address, f"Request error: {e}")
    except json.JSONDecodeError:
        log_error(wallet_address, "Failed to decode JSON from response.")
    return None

def get_current_block():
    """Fetch the current block number from Polygonscan."""
    query_params = {
        'module': 'proxy',
        'action': 'eth_blockNumber',
        'apikey': api_key
    }
    url_parts = ('https', 'api.polygonscan.com', '/api', '', urlencode(query_params), '')
    url = urlunparse(url_parts)
    try:
        response = requests.get(url)
        response.raise_for_status()
        block_data = response.json()
        if 'result' in block_data:
            return int(block_data['result'], 16)  # Convert hex block number to integer
    except requests.RequestException as e:
        log_error('current_block', f"Request error: {e}")
    except json.JSONDecodeError:
        log_error('current_block', "Failed to decode JSON from response.")
    return None

def fetch_tx_by_address(wallet_address, start_block, end_block, chunk_size, depth=0, max_depth=1):
    """Fetch transactions for a specific address from Polygonscan and save them in SQL table."""
    if wallet_address in processed_wallets or depth > max_depth:
        return
    
    processed_wallets.add(wallet_address)
    start_time = time.time()
    page = 1
    new_wallets = set()
    total_transactions = 0
    
    while True:
        query_params = {
            'module': 'account',
            'action': 'txlist',
            'address': wallet_address,
            'startblock': start_block,
            'endblock': end_block,
            'sort': 'asc',
            'apikey': api_key,
            'page': page,
            'offset': chunk_size
        }
        url_parts = ('https', 'api.polygonscan.com', '/api', '', urlencode(query_params), '')
        url = urlunparse(url_parts)
        try:
            response = requests.get(url)
            response.raise_for_status()
            tx_data = response.json()
            if 'result' in tx_data and isinstance(tx_data['result'], list):
                if not tx_data['result']:
                    break
                transactions = tx_data['result']
                total_transactions += len(transactions)
                save_to_sql(transactions)
                logging.info(f"Data has been written to the SQL database. Total transactions pulled so far: {total_transactions}")
                for tx in transactions:
                    if tx['to'] and tx['to'] not in processed_wallets:
                        new_wallets.add(tx['to'])
                    if tx['from'] and tx['from'] not in processed_wallets:
                        new_wallets.add(tx['from'])
                page += 1
            else:
                logging.warning("Failed to fetch transactions.")
                break
        except requests.RequestException as e:
            log_error(wallet_address, f"Request error: {e}")
            break
        except json.JSONDecodeError:
            log_error(wallet_address, "Failed to decode JSON from response.")
            break
        
        time.sleep(0.2)
    
    end_time = time.time()
    total_time = end_time - start_time
    logging.info(f"Total time taken: {total_time:.2f} seconds")

    if depth < max_depth:
        for new_wallet in new_wallets:
            new_start_block = get_starting_block(new_wallet)
            if new_start_block is not None:
                fetch_tx_by_address(new_wallet, new_start_block, end_block, chunk_size, depth + 1, max_depth)

def save_to_sql(transactions):
    """Save the transactions data to an Azure SQL Database table."""
    conn = pyodbc.connect(
        f'DRIVER={driver};SERVER={server};PORT=1433;DATABASE={database};UID={username};PWD={password}')
    cursor = conn.cursor()

    # Create table if it does not exist
    cursor.execute("""
    IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='Transactions' AND xtype='U')
    CREATE TABLE Transactions (
        hash NVARCHAR(66),
        nonce BIGINT,
        blockHash NVARCHAR(66),
        blockNumber BIGINT,
        transactionIndex INT,
        fromAddress NVARCHAR(42),
        toAddress NVARCHAR(42),
        value DECIMAL(38,0),
        gas DECIMAL(38,0),
        gasPrice DECIMAL(38,0),
        isError INT,
        txreceipt_status INT,
        input NVARCHAR(MAX),
        contractAddress NVARCHAR(42),
        cumulativeGasUsed DECIMAL(38,0),
        gasUsed DECIMAL(38,0),
        confirmations BIGINT,
        timestamp BIGINT
    )
    """)

    for tx in transactions:
        cursor.execute("""
        INSERT INTO Transactions (
            hash, nonce, blockHash, blockNumber, transactionIndex, fromAddress, toAddress, value, gas, gasPrice,
            isError, txreceipt_status, input, contractAddress, cumulativeGasUsed, gasUsed, confirmations, timestamp
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            tx.get('hash'),
            int(tx.get('nonce', 0)),
            tx.get('blockHash'),
            int(tx.get('blockNumber', 0)),
            int(tx.get('transactionIndex', 0)),
            tx.get('from'),
            tx.get('to'),
            Decimal(tx.get('value', 0)),
            Decimal(tx.get('gas', 0)),
            Decimal(tx.get('gasPrice', 0)),
            int(tx.get('isError', 0)),
            int(tx.get('txreceipt_status', 0)),
            tx.get('input'),
            tx.get('contractAddress'),
            Decimal(tx.get('cumulativeGasUsed', 0)),
            Decimal(tx.get('gasUsed', 0)),
            int(tx.get('confirmations', 0)),
            int(tx.get('timeStamp', 0))
        ))

    conn.commit()
    cursor.close()
    conn.close()

if __name__ == "__main__":
    # Prompt for user inputs
    wallet_address = input("Enter the wallet address: ")

    # Automatically determine the starting block number
    start_block = get_starting_block(wallet_address)
    if start_block is None:
        logging.error("Could not determine the starting block number. Exiting.")
        exit()

    # Automatically determine the current block number
    end_block = get_current_block()
    if end_block is None:
        logging.error("Could not determine the current block number. Exiting.")
        exit()

    # Set a default chunk size or prompt for it
    chunk_size = 1000  # 1000 transactions per request, adjust as needed

    # Fetch transactions for the wallet address
    fetch_tx_by_address(wallet_address, start_block, end_block, chunk_size)
