import requests
import json
import os
import time

# Retrieve the Polyscan API key from environment variables
api_key = os.getenv('POLYSCAN_API_KEY')

# Define the directory where output JSON files will be stored
directory = 'data'

# Check if the directory exists and create it if necessary
if not os.path.exists(directory):
    os.makedirs(directory)

def fetch_tx(start_block, end_block, chunk_size):
    """ 
    Parameters:
        api_key (str): The Polyscan API key used for making API requests.
        start_block (int): The starting block number of the range to fetch transactions from.
        end_block (int): The ending block number of the range to fetch transactions from.
        chunk_size (int): The number of blocks to query in each API call.

    Returns:
        None

    Files created:
        Each chunk of transactions is written to a separate file in the 'data' directory. 
        Files are named using the format 'blk_{start_block}_{end_block}.json'.
    """
    # Initialize timer for stats
    start = time.time()
    
    # Iterate over the specified block range in chunks
    for chunk_start in range(start_block, end_block + 1, chunk_size):
        transactions = []
        chunk_end = min(chunk_start + chunk_size - 1, end_block)
        
        # Fetch transactions for each block in the current chunk
        for block in range(chunk_start, chunk_end + 1):
            url = f'https://api.polygonscan.com/api?module=proxy&action=eth_getBlockByNumber&tag={hex(block)}&boolean=true&apikey={api_key}'
            try:
                response = requests.get(url)
                response.raise_for_status()
                block_data = response.json()
                if 'result' in block_data and isinstance(block_data['result'], dict):
                    transactions.extend(block_data['result'].get('transactions', []))
            except requests.RequestException as e:
                print(f"Request error: {e}")
            except json.JSONDecodeError:
                print("Failed to decode JSON from response.")
            
        # Write the collected transactions for the current chunk to a JSON file
        filename = f'{directory}/blk_{chunk_start}_{chunk_end}.json'
        with open(filename, 'w') as file:
            json.dump(transactions, file, indent=4)
        
        print(f"Data has been written to {filename}")
        
    # Calculate and print time statistics after all chunks are processed
    end = time.time()
    total_time = end - start
    average_time = total_time / (end_block - start_block + 1)
    
    print(f"Total time taken: {total_time:.2f} seconds")
    print(f"Average time per block: {average_time:.2f} seconds")

# Parameters for the block range and chunk size
start_block = 19808000  # Starting block number
end_block   = 19808024  # Ending block number
chunk_size  = 5         # Number of blocks to query in each API call

# Fetch and save transactions
fetch_tx(start_block, end_block, chunk_size)
