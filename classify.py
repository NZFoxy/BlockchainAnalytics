def calculate_fraud_score(row):
    score = 0
    
    avg_gas = 21000  # Example average gas for a simple transaction
    if int(row['gasUsed']) > avg_gas * 5:
        score += 0.2
    
    if int(row['isError']) == 1 or int(row['txreceipt_status']) == 0:
        score += 0.3
    
    avg_value = 10 * (10**18)  # Example average value in Weis
    if int(row['value']) > avg_value * 100:
        score += 0.2
    
    if int(row['confirmations']) < 10:
        score += 0.1
    
    if int(row['nonce']) > 100:  # Example threshold
        score += 0.1
    
    if False and row['contractAddress'] and is_new_contract(row['contractAddress']):
        score += 0.2
    
    if False and is_non_standard_input(row['input']):
        score += 0.1
    
    return min(score, 1)

def is_new_contract(contract_address):
    # Implement logic to check if contract is new or empty
    return False

def is_non_standard_input(input_data):
    # Implement logic to analyze input data
    return False
