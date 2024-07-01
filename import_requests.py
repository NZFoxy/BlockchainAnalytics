import os
import requests
import json
import time
from urllib.parse import urlencode, urlunparse

# Ensure API key is set
api_key = os.getenv('POLYGONSCAN_API_KEY')
if not api_key:
    raise ValueError("API Key is not set in environment variables")

# Directory for output
data_directory = 'data'
if not os.path.exists(data_directory):
    os.makedirs(data_directory)

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
        'error_message': error_message
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
    print(f"Error logged for address {wallet_address}")

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
        'offset': 1
    }
    url_parts = ('https', 'api.polygonscan.com', '/api', '', urlencode(query_params), '')
    url = urlunparse(url_parts)
    try:
        response = requests.get(url)
        response.raise_for_status()
        tx_data = response.json()
        if 'result' in tx_data and isinstance(tx_data['result'], list) and len(tx_data['result']) > 0:
            return int(tx_data['result'][0]['blockNumber'])
        else:
            print("No transactions found for this address.")
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
            return int(block_data['result'], 16)  # Convert hex to int
    except requests.RequestException as e:
        log_error('current_block', f"Request error: {e}")
    except json.JSONDecodeError:
        log_error('current_block', "Failed to decode JSON from response.")
    return None

def fetch_tx_by_address(wallet_address, start_block, end_block, chunk_size, depth=0, max_depth=2):
    """Fetch transactions for a specific address from Polygonscan and save them in JSON files."""
    if wallet_address in processed_wallets or depth > max_depth:
        return
    
    processed_wallets.add(wallet_address)
    start = time.time()
    page = 1
    new_wallets = set()
    total_transactions = 0  # Initialize counter for transactions
    
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
                    break  # Exit loop if no more transactions are found
                transactions = tx_data['result']
                total_transactions += len(transactions)  # Update counter
                filename = f'{data_directory}/tx_{wallet_address}_{page}.json'
                with open(filename, 'w') as file:
                    json.dump(transactions, file, indent=4)
                print(f"Data has been written to {filename}")
                print(f"Total transactions pulled so far: {total_transactions}")
                for tx in transactions:
                    new_wallets.add(tx['to'])
                    new_wallets.add(tx['from'])
                page += 1
            else:
                print("Failed to fetch transactions.")
                break
        except requests.RequestException as e:
            log_error(wallet_address, f"Request error: {e}")
            break
        except json.JSONDecodeError:
            log_error(wallet_address, "Failed to decode JSON from response.")
            break
        
        # Sleep to respect rate limits (5 calls/second)
        #time.sleep(0.2)  # 0.2 seconds sleep to stay within 5 calls per second
    
    end = time.time()
    total_time = end - start
    print(f"Total time taken: {total_time:.2f} seconds")

    # Recursively fetch transactions for new wallet addresses
    if depth < max_depth:
        for new_wallet in new_wallets:
            if new_wallet and new_wallet not in processed_wallets:
                new_start_block = get_starting_block(new_wallet)
                if new_start_block is not None:
                    fetch_tx_by_address(new_wallet, new_start_block, end_block, chunk_size, depth + 1, max_depth)

# Prompt for user inputs
wallet_address = input("Enter the wallet address: ")

# Automatically determine the starting block number
start_block = get_starting_block(wallet_address)
if start_block is None:
    print("Could not determine the starting block number. Exiting.")
    exit()

# Automatically determine the current block number
end_block = get_current_block()
if end_block is None:
    print("Could not determine the current block number. Exiting.")
    exit()

# Set a default chunk size or prompt for it
chunk_size = 1000  # 1000 transactions per request, adjust as needed

# Fetch transactions for the wallet address
fetch_tx_by_address(wallet_address, start_block, end_block, chunk_size)
