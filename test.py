import pyodbc

server = 'autblockchain.database.windows.net'
database = 'TransactionsAll'
username = 'CloudSAcab2136b'
password = 'Bottles1!' #Ask Daniel
driver = '{ODBC Driver 18 for SQL Server}'

##Connect to the SQL Server
with pyodbc.connect('DRIVER=' + driver + ';SERVER=tcp:' + server + ';PORT=1433;DATABASE=' + database + ';UID=' + username + ';PWD=' + password) as conn:
    with conn.cursor() as cursor:
        # Execute a test query to see if connection works
        cursor.execute("SELECT TOP 3 name, collation_name FROM sys.databases")
        row = cursor.fetchone()
        while row:
            print(str(row[0]) + " " + str(row[1]))
            row = cursor.fetchone()

        # Drop the Transactions table
        cursor.execute("DROP TABLE IF EXISTS Transactions")

        # Check if the table still exists
        cursor.execute("""
        IF OBJECT_ID('Transactions', 'U') IS NOT NULL
            SELECT 'Table still exists'
        ELSE
            SELECT 'Table has been dropped'
        """)

##Fetch and print the result of the existence check
        result = cursor.fetchone()
        if result:
            print(result[0])
