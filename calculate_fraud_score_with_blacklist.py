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

    # Only process fromAddress and toAddress
    from_address = row[5]  # Assuming column 6 is fromAddress
    to_address = row[6]  # Assuming column 7 is toAddress

    # Check if the fromAddress or toAddress is blacklisted
    if is_address_blacklisted(cursor_fraud, from_address) or is_address_blacklisted(cursor_fraud, to_address):
        score += 0.8

    avg_gas = 21000  # Example average gas for a simple transaction

    try:
        # Convert gasUsed from hexadecimal to integer
        gas_used = int(row[16], 16)  # Assuming column 17 is gasUsed
        if gas_used > avg_gas * 5:
            score += 0.3
    except ValueError:
        print(f"Invalid gasUsed value: {row[16]}")
        pass

    if int(row[10]) == 1 or int(row[11]) == 0:
        score += 0.1

    avg_value = 10 * (10**18)  # Example average value in Wei
    if float(row[7]) > avg_value * 100:
        score += 0.4

    if int(row[17]) < 20000:
        score += 0.1

    try:
        # Convert nonce from hexadecimal to integer
        nonce = int(row[1])
        if nonce > 100:  # Example threshold
            score += 0.1
    except ValueError:
        print(f"Invalid nonce value: {row[1]}")
        pass

    return min(score, 1)  # Cap the score at 1

# Example usage
def main():
    # Connect to both the transactions and fraud_wallets databases
    conn_tx = sqlite3.connect('transactions.db')
    cursor_tx = conn_tx.cursor()

    conn_fraud = sqlite3.connect('fraud_wallets.db')
    cursor_fraud = conn_fraud.cursor()

    # Adjust the cursor fetch size to process all entries
    cursor_tx.arraysize = 10000  # Adjust as needed

    # Example query to get a row from the transactions database
    cursor_tx.execute('SELECT hash, nonce, blockHash, blockNumber, transactionIndex, fromAddress, toAddress, value, gas, gasPrice, isError, txreceipt_status, gasUsed, confirmations, timestamp FROM Transactions')
    transactions = cursor_tx.fetchall()

    for example_row in transactions:
        # Get the fromAddress and toAddress from the transactions
        from_address = example_row[5]  # Assuming column 6 is fromAddress
        to_address = example_row[6]  # Assuming column 7 is toAddress

        # Check if either address matches a fraudulent address
        if is_address_blacklisted(cursor_fraud, from_address) or is_address_blacklisted(cursor_fraud, to_address):
            # If a match is found, output the transaction
            print(f"Fraudulent transaction: {example_row['hash']}")

    # Close the connections
    conn_tx.close()
    conn_fraud.close()

if __name__ == "__main__":
    main()