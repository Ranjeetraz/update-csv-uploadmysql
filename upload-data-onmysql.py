import os
import pandas as pd
import mysql.connector
from mysql.connector import Error

# MySQL connection details
mydb = mysql.connector.connect(
    host="localhost",
    user="user",
    password="password",
    database="databasename"
)

# Directory containing the CSV files
csv_directory = r"C:\Users\IHQ-All-csv"

def insert_data(cursor, table_name, df):
    date_columns = ['created_at', 'updated_at', 'shipment_date', 'expected_date', ''] 
    for col in date_columns:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors='coerce')  
    
   
    columns = ', '.join(df.columns)
    placeholders = ', '.join(['%s'] * len(df.columns))
    insert_query = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
    
    
    for row in df.itertuples(index=False):
        cursor.execute(insert_query, tuple(row))

try:
    cursor = mydb.cursor()

    
    csv_files = [f for f in os.listdir(csv_directory) if f.endswith('.csv')]

    for csv_file in csv_files:
        table_name = os.path.splitext(csv_file)[0]  
        csv_path = os.path.join(csv_directory, csv_file)
        
        # Read CSV file into DataFrame
        df = pd.read_csv(csv_path)
        
        # Insert data into the corresponding table
        insert_data(cursor, table_name, df)
        
        mydb.commit()
        
    print("Data has been successfully uploaded to MySQL.")
    
except Error as e:
    print(f"Error: {e}")
    mydb.rollback()
finally:
    cursor.close()
    mydb.close()
