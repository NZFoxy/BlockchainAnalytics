import sqlite3

# Connect to the transactions database
conn = sqlite3.connect('Transactions.db')
cursor = conn.cursor()

# Execute a query to retrieve all table names
cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
tables = cursor.fetchall()

# Print all table names
print("Tables in the database:", tables)

# Close the connection
conn.close()
