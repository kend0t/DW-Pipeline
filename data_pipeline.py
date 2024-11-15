# Data Pipeline

# Importing necessary libraries
import pandas as pd
import numpy as np


# Loading and Reading CSV file 
pd.reset_option('display.max_rows', None)
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


# Computing for the unit price:
# Creating a dataframe for rows with complete column values
complete_transactions = modified_data.dropna()

# Extracting one row for each unique product along with its price and quantity
unique_products = complete_transactions.drop_duplicates(subset=['product_id'],keep='first')
unique_products = unique_products.drop(columns=['transaction_id','timestamp'])

# Computation for unit price
unique_products['unit_price'] = (unique_products['price'] / unique_products['quantity'])


# Fill the missing prices and quantities
final_unique_products = unique_products.drop(columns=['quantity','price'])
modified_data = modified_data.merge(final_unique_products, on='product_id', how='left')

modified_data['price'] = modified_data['price'].fillna((modified_data['unit_price'] * modified_data['quantity']).round(2))
modified_data['quantity'] = modified_data['quantity'].fillna((modified_data['price'] / modified_data['unit_price']).apply(np.floor))

# Dropping unit_price column to retain original table format
modified_data = modified_data.drop(columns=['unit_price'])


# Adjusting the date format:
# Change the / separator to - 
modified_data['timestamp'] = modified_data['timestamp'].str.replace('/','-')

# Removing rows with null values for both quantity and price
tentative_change = modified_data.copy()
tentative_change = tentative_change.dropna(subset=['quantity','price'], how='all')

# Resetting the transaction IDs to observe proper order
tentative_change['transaction_id'] = ['T{:05d}'.format(i) for i in range(len(tentative_change))]