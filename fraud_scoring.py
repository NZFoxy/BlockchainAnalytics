def calculate_fraud_score(row):
    score = 0
    
    avg_gas = 21000  # Example average gas for a simple transaction
    if int(row['gasUsed']) > avg_gas * 5:
        score += 0.3
    
    if int(row['isError']) == 1 or int(row['txreceipt_status']) == 0:
        score += 0.1
    
    avg_value = 10 * (10**18)  # Example average value in Wei
    if int(row['value']) > avg_value * 100:
        score += 0.4
    
    if int(row['confirmations']) < 20000:
        score += 0.1
    
    if int(row['nonce']) > 100:  # Example threshold
        score += 0.1

    return min(score, 1)  # Cap the score at 1
