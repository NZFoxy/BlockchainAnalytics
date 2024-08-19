import sqlite3
import csv

# Step 1: Define the path to the CSV file
csv_file_path = 'blacklist-mixers.csv'

# Step 2: Connect to the SQLite database (or create it if it doesn't exist)
conn = sqlite3.connect('fraud_wallets.db')
cursor = conn.cursor()

# Step 3: Create the Fraud_Wallets table if it doesn't exist
cursor.execute('''
CREATE TABLE IF NOT EXISTS Fraud_Wallets (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    address TEXT UNIQUE NOT NULL,
    identified_date DATE NOT NULL,
    notes TEXT
)
''')

# Step 4: Open the CSV file and read its contents
with open(csv_file_path, newline='') as csvfile:
    csv_reader = csv.DictReader(csvfile)
    
    # Step 5: Insert each row from the CSV file into the Fraud_Wallets table
    for row in csv_reader:
        cursor.execute('''
        INSERT OR IGNORE INTO Fraud_Wallets (address, identified_date, notes)
        VALUES (?, ?, ?)
        ''', (row['Address'], row['Identified_Date'], row['Notes']))

# Step 6: Commit the changes to the database
conn.commit()

# Step 7: Query the database to verify the import
cursor.execute('SELECT * FROM Fraud_Wallets')
records = cursor.fetchall()

# Step 8: Print the records to verify
print("Records in Fraud_Wallets table:")
for record in records:
    print(record)

# Step 9: Close the connection
conn.close()
