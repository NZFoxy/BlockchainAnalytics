"""
classify.py

This module contains a function to calculate a fraud score for blockchain transactions
based on a set of predefined heuristics. The score is calculated by analyzing various
aspects of the transaction, such as gas used, value, confirmations, and other indicators
of potential fraudulent activity. The function is designed to be flexible, allowing
different weights and thresholds to be configured as needed.

The function returns a fraud score between 0 and 1, with higher scores indicating
a higher likelihood of fraudulent activity.
"""

def calculate_fraud_score(
        row: dict, 
        avg_gas: int = 21000, 
        gas_multiplier: int = 5, 
        gas_weight: float = 0.3, 
        avg_value: int = 10 * (10**18), 
        value_multiplier: int = 100, 
        value_weight: float = 0.4, 
        min_confirmations: int = 20000, 
        confirmations_weight: float = 0.1, 
        max_nonce: int = 100, 
        nonce_weight: float = 0.1, 
        error_weight: float = 0.1) -> float:

    """
    Calculate the fraud score for a blockchain transaction based on various heuristics.

    Parameters:
    - row (dict): A dictionary containing transaction details.
    - avg_gas (int): Average gas used for a simple transaction. Default is 21000.
    - gas_multiplier (int): Multiplier for gas to determine a suspicious threshold. Default is 5.
    - gas_weight (float): Weight for gas used in calculating the fraud score. Default is 0.3.
    - avg_value (int): Average value in Wei for transactions. Default is 10 * (10**18).
    - value_multiplier (int): Multiplier for value to determine a suspicious threshold. Default is 100.
    - value_weight (float): Weight for value used in calculating the fraud score. Default is 0.4.
    - min_confirmations (int): Minimum confirmations required to reduce suspicion. Default is 20000.
    - confirmations_weight (float): Weight for confirmations used in calculating the fraud score. Default is 0.1.
    - max_nonce (int): Maximum nonce value considered normal. Default is 100.
    - nonce_weight (float): Weight for nonce used in calculating the fraud score. Default is 0.1.
    - error_weight (float): Weight for transaction errors in calculating the fraud score. Default is 0.1.

    Returns:
    - float: A fraud score between 0 and 1.
    """
    try:
        score = 0
        
        # Check if the gas used exceeds the suspicious threshold
        if int(row.get('gasUsed', 0)) > avg_gas * gas_multiplier:
            score += gas_weight
        
        # Check if the transaction had any errors
        if int(row.get('isError', 0)) == 1 or int(row.get('txreceipt_status', 1)) == 0:
            score += error_weight
        
        # Check if the value of the transaction exceeds the suspicious threshold
        if int(row.get('value', 0)) > avg_value * value_multiplier:
            score += value_weight
        
        # Check if the number of confirmations is below the minimum threshold
        if int(row.get('confirmations', 0)) < min_confirmations:
            score += confirmations_weight
        
        # Check if the nonce exceeds the normal range
        if int(row.get('nonce', 0)) > max_nonce:
            score += nonce_weight

        # Cap the score at 1 to ensure it remains within the 0-1 range
        return min(score, 1)
    
    except Exception as e:
        # Handle any unexpected errors and return a neutral score
        print(f"Error calculating fraud score: {e}")
        return 0.0
