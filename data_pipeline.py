# Data Pipeline

# Importing necessary libraries
import pandas as pd
import numpy as np
from dateutil.parser import parse
from datetime import datetime
import sqlite3


# Loading and Reading CSV file 
transactions = pd.read_csv('inconsistent_transactions.csv') 


# Creating a copy of the original transaction data
modified_data = transactions.copy()


# Extending the range of transaction IDS
modified_data['transaction_id'] = modified_data['transaction_id'].str.replace(r'^T','T0', regex=True)


# Enforcing a single Product ID format
modified_data['product_id'] = modified_data['product_id'].str.replace(' ','').str.replace('-','').str.replace('p','P')


# Changing the price format
modified_data['price'] = modified_data['price'].str.replace('$','')
modified_data['price'] = modified_data['price'].astype(float).round(2)

# Formating the timestamp
def new_timestamp(row):
    try:
        if '-' in row and row.count('-') == 2:
            try:
                ts = datetime.strptime(row, '%d-%m-%Y')
                return ts.strftime('%Y-%m-%d') 
            except ValueError:
                pass  
        
        ts = parse(row)
        return ts.strftime('%Y-%m-%d')
    
    except ValueError:
        return row  

modified_data['timestamp'] = modified_data['timestamp'].apply(new_timestamp)


# Reformating the transaction IDs in chronological order
modified_data = modified_data.sort_values(by='timestamp')
modified_data['transaction_id'] = ['T{:05d}'.format(i) for i in range(len(modified_data))]


# Computing for the unit price:
# Creating a dataframe for rows with complete column values
complete_transactions = modified_data.dropna()
unique_products = complete_transactions.copy()

# Computation for unit price
unique_products['unit_price'] = (unique_products['price'] / unique_products['quantity'])

# Acquiring the highest unit price for each product (We are assuming that huge fluctuations in prices are caused by discounts)
unique_products['max_unit_price'] = unique_products.groupby('product_id')['unit_price'].transform('max')
unique_products = unique_products.drop_duplicates(subset='product_id')
unique_products = unique_products.drop(columns=['transaction_id','quantity','price','timestamp','unit_price'])

# Remove rows in the modified_data data frame that have null values for both price and quantity
modified_data = modified_data.dropna(subset=['quantity','price'], how='all')


# Fill the missing prices and quantities
modified_data = modified_data.merge(unique_products, on='product_id', how='left')

# Computation for missing price values
modified_data['price'] = modified_data['price'].fillna((modified_data['max_unit_price'] * modified_data['quantity']).round(2))

# Computation for missing quantity values
modified_data['quantity'] = modified_data['quantity'].fillna((modified_data['price'] / modified_data['max_unit_price']).apply(np.floor))
modified_data['quantity'] = modified_data['quantity'].replace(0, 1)

# Change quantity to int
modified_data['quantity'] = modified_data['quantity'].astype(int)
modified_data

# Dropping max_unit_price column to retain original data format
modified_data = modified_data.drop(columns=['max_unit_price'])


# - - - - - - - - - - - - - - - - - - - - - - - - - - - 
# Loading into the database
conn = sqlite3.connect('shopease_sales.db')
cursor = conn.cursor()

cursor.execute('''
    CREATE TABLE IF NOT EXISTS sales (
        transaction_id TEXT PRIMARY KEY,
        product_id TEXT,
        quantity REAL,
        price REAL,
        timestamp TEXT
    )
''')
conn.commit()

for _, row in modified_data.iterrows():
    cursor.execute('''
        INSERT OR REPLACE INTO sales (transaction_id, product_id, quantity, price, timestamp)
        VALUES (?, ?, ?, ?, ?)
    ''', (row['transaction_id'], row['product_id'], row['quantity'], row['price'], row['timestamp']))
conn.commit()

cursor.execute('SELECT * FROM sales LIMIT 5')
headers = [description[0] for description in cursor.description]
rows = cursor.fetchall()

print("\t".join(headers))

for row in rows:
    print("\t".join(str(cell) for cell in row))

conn.close()
