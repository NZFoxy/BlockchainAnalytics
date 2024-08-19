import os
import requests
import json
import time
from urllib.parse import urlencode, urlunparse
import sqlite3
from decimal import Decimal
import logging

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Ensure API key is set
api_key = os.getenv('POLYGONSCAN_API_KEY')
if not api_key:
    raise ValueError("API Key is not set in environment variables")

# Ensure the Database directory exists
database_dir = 'Database'
if not os.path.exists(database_dir):
    os.makedirs(database_dir)

# SQLite database connection details
database = os.path.join(database_dir, 'transactions.db')

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

def ensure_transactions_table_exists():
    """Ensure the Transactions table exists in the SQLite database."""
    conn = sqlite3.connect(database)
    cursor = conn.cursor()
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS Transactions (
        hash TEXT PRIMARY KEY,
        nonce INTEGER,
        blockHash TEXT,
        blockNumber INTEGER,
        transactionIndex INTEGER,
        fromAddress TEXT,
        toAddress TEXT,
        value TEXT,
        gas TEXT,
        gasPrice TEXT,
        isError INTEGER,
        txreceipt_status INTEGER,
        input TEXT,
        contractAddress TEXT,
        cumulativeGasUsed TEXT,
        gasUsed TEXT,
        confirmations INTEGER,
        timestamp INTEGER
    )
    """)
    conn.commit()
    cursor.close()
    conn.close()

def filter_unique_transactions(transactions):
    """Filter out transactions that already exist in the database."""
    conn = sqlite3.connect(database)
    cursor = conn.cursor()
    
    unique_transactions = []
    for tx in transactions:
        cursor.execute("SELECT COUNT(*) FROM Transactions WHERE hash = ?", (tx.get('hash'),))
        if cursor.fetchone()[0] == 0:
            unique_transactions.append(tx)
    
    cursor.close()
    conn.close()
    return unique_transactions

def save_to_sql(transactions):
    """Save the transactions data to an SQLite Database table."""
    conn = sqlite3.connect(database)
    cursor = conn.cursor()

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
            str(tx.get('value', 0)),
            str(tx.get('gas', 0)),
            str(tx.get('gasPrice', 0)),
            int(tx.get('isError', 0)),
            int(tx.get('txreceipt_status', 0)),
            tx.get('input'),
            tx.get('contractAddress'),
            str(tx.get('cumulativeGasUsed', 0)),
            str(tx.get('gasUsed', 0)),
            int(tx.get('confirmations', 0)),
            int(tx.get('timeStamp', 0))
        ))

    conn.commit()
    cursor.close()
    conn.close()

def fetch_tx_by_address(wallet_address, start_block, end_block, chunk_size, depth=0, max_depth=1):
    """Fetch transactions for a specific address from Polygonscan and save them in SQL table."""
    if wallet_address in processed_wallets or depth > max_depth:
        return
    
    # Ensure the Transactions table exists
    ensure_transactions_table_exists()
    
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
            'offset': chunk_size,
        }
        url_parts = ('https', 'api.polygonscan.com', '/api', '', urlencode(query_params), '')
        url = urlunparse(url_parts)
        try:
            response = requests.get(url)
            response.raise_for_status()
            tx_data = response.json()
            if 'result' in tx_data and isinstance(tx_data['result'], list):
                transactions = tx_data['result']

                if not transactions:
                    logging.info("No more transactions found. Exiting pagination loop.")
                    break
                
                # Filter out transactions that already exist in the database
                unique_transactions = filter_unique_transactions(transactions)
                total_transactions += len(unique_transactions)
                save_to_sql(unique_transactions)
                logging.info(f"Data has been written to the SQL database. Total transactions pulled so far: {total_transactions}")
                
                for tx in transactions:
                    if tx['to'] and tx['to'] not in processed_wallets:
                        new_wallets.add(tx['to'])
                    if tx['from'] and tx['from'] not in processed_wallets:
                        new_wallets.add(tx['from'])
                
                page += 1
                # Update start_block to prevent fetching duplicate transactions
                start_block = int(transactions[-1]['blockNumber']) + 1
            else:
                logging.warning("Failed to fetch transactions. Exiting.")
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
