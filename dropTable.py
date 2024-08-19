import sqlite3

# Step 1: Connect to the SQLite database
conn = sqlite3.connect('fraud_wallets.db')
cursor = conn.cursor()

# Step 2: Drop the Fraud_Wallets table if it exists
cursor.execute('''
DROP TABLE IF EXISTS Fraud_Wallets
''')

# Step 3: Commit the changes
conn.commit()

# Step 4: Verify that the table has been dropped
try:
    cursor.execute('SELECT * FROM Fraud_Wallets')
    records = cursor.fetchall()
    print("Table still exists, and records are:", records)
except sqlite3.OperationalError as e:
    print("Table does not exist. It has been successfully dropped.")

# Step 5: Close the connection
conn.close()
