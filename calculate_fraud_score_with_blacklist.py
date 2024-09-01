import sqlite3

# Function to check if the address is in the Fraud_Wallets database
def is_address_blacklisted(cursor_fraud, address):
    # Query to check if the address exists in Fraud_Wallets
    cursor_fraud.execute('SELECT 1 FROM Fraud_Wallets WHERE address = ?', (address,))
    result = cursor_fraud.fetchone()
    
    # If result is not None, the address is blacklisted
    return result is not None

# Function to calculate the fraud score

def calculate_fraud_score(cursor_fraud, row):
    score = 0
    
    # Only process fromAddress and toAddress, ignore contractAddress
    from_address = row['fromAddress']
    to_address = row['toAddress']
    
    # Check if the fromAddress or toAddress is blacklisted
    if is_address_blacklisted(cursor_fraud, from_address) or is_address_blacklisted(cursor_fraud, to_address):
        score += 0.8  # Add to the score if the address is blacklisted
    
    avg_gas = 21000  # Example average gas for a simple transaction
    if row['gasUsed'] and float(row['gasUsed']) > avg_gas * 5:
        score += 0.3
    
    if int(row['isError']) == 1 or int(row['txreceipt_status']) == 0:
        score += 0.1
    
    avg_value = 10 * (10**18)  # Example average value in Wei
    if float(row['value']) > avg_value * 100:
        score += 0.4
    
    if int(row['confirmations']) < 20000:
        score += 0.1
    
    # Convert the nonce from hexadecimal to an integer
    try:
        nonce = int(row['nonce'])  # Convert from hex (base 16) to decimal
        if nonce > 100:  # Example threshold
            score += 0.1
    except ValueError:
        print(f"Invalid nonce value: {row['nonce']}")
        pass

    return min(score, 1)  # Cap the score at 1

# Example usage
def main():
    # Connect to both the transactions and fraud_wallets databases
    conn_tx = sqlite3.connect('transactions.db')
    cursor_tx = conn_tx.cursor()
    
    conn_fraud = sqlite3.connect('fraud_wallets.db')
    cursor_fraud = conn_fraud.cursor()
    
    # Example query to get a row from the transactions database
    cursor_tx.execute('SELECT fromAddress, toAddress, gasUsed, isError, txreceipt_status, value, confirmations, nonce FROM Transactions')
    transactions = cursor_tx.fetchall()
    
    for example_row in transactions:
        # Map the row to a dictionary with correct field names
        example_row_dict = {
            'fromAddress': example_row[0],  # 'fromAddress'
            'toAddress': example_row[1],    # 'toAddress'
            'gasUsed': example_row[2],      # 'gasUsed'
            'isError': example_row[3],      # 'isError'
            'txreceipt_status': example_row[4],  # 'txreceipt_status'
            'value': example_row[5],         # 'value'
            'confirmations': example_row[6], # 'confirmations'
            'nonce': example_row[7]          # 'nonce'
        }
        
        # Calculate the fraud score for the example row
        fraud_score = calculate_fraud_score(cursor_fraud, example_row_dict)
        print(f"Fraud Score for Transaction {example_row[0]}: {fraud_score}")

    # Close the connections
    conn_tx.close()
    conn_fraud.close()

if __name__ == "__main__":
    main()
