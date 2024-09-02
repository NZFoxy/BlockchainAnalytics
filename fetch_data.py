import sqlite3
import pandas as pd

def fetch_data_from_sql(database_path):
    conn = sqlite3.connect(database_path)
    query = "SELECT * FROM Transactions"
    df = pd.read_sql_query(query, conn)
    conn.close()
    return df
#sd