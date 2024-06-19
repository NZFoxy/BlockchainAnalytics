import os
import requests
import json
import time
from urllib.parse import urlencode, urlunparse

# Ensure API key is set
# This script requires an environment variable 'POLYGONSCAN_API_KEY' to be set with your Polygonscan API key.
api_key = os.getenv('POLYGONSCAN_API_KEY')
if not api_key:
    raise ValueError("API Key is not set in environment variables")

# Directory for output
# Create a directory named 'data' to store the fetched transactions.
directory = 'data'
if not os.path.exists(directory):
    os.makedirs(directory)

def get_starting_block(wallet_address):
    """Fetch the starting block number for a specific address from Polygonscan.

    Args:
        wallet_address (str): The wallet address for which to fetch the starting block.

    Returns:
        int: The starting block number containing the first transaction of the wallet.
        None: If no transactions are found or an error occurs.
    """
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
        print(f"Request error: {e}")
    except json.JSONDecodeError:
        print("Failed to decode JSON from response.")
    return None

def get_current_block():
    """Fetch the current block number from Polygonscan.

    Returns:
        int: The current block number.
        None: If an error occurs.
    """
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
        print(f"Request error: {e}")
    except json.JSONDecodeError:
        print("Failed to decode JSON from response.")
    return None

def fetch_tx_by_address(wallet_address, start_block, end_block, chunk_size):
    """Fetch transactions for a specific address from Polygonscan and save them in JSON files.

    Args:
        wallet_address (str): The wallet address for which to fetch transactions.
        start_block (int): The block number to start fetching transactions from.
        end_block (int): The block number to stop fetching transactions at.
        chunk_size (int): The number of transactions to fetch per API request.
    """
    start = time.time()
    
    page = 1
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
                filename = f'{directory}/tx_{wallet_address}_{page}.json'
                with open(filename, 'w') as file:
                    json.dump(transactions, file, indent=4)
                print(f"Data has been written to {filename}")
                page += 1
            else:
                print("Failed to fetch transactions.")
                break
        except requests.RequestException as e:
            print(f"Request error: {e}")
            break
        except json.JSONDecodeError:
            print("Failed to decode JSON from response.")
            break
        
        # Sleep to respect rate limits (5 calls/second)
        time.sleep(0.2)  # 0.2 seconds sleep to stay within 5 calls per second
    
    end = time.time()
    total_time = end - start
    print(f"Total time taken: {total_time:.2f} seconds")

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
