# Enery Contract Pricing Model
This is an Enery Contract Pricing Model use case project for Data Engineering

## Create and activate a virtual environment (recommended but optional)
For windows:
1. Set up a virtual environment `python -m venv venv`
2. Activate the virtual environment `venv\Scripts\activate`

## How to run this python solution
1. When running for the first time, please install the required packages in the `requirements.txt` file using pip
```
pip install -r requirements.txt
```
2. Run the python script
```
python energy_contract.py
```

## Tables structure for the cocktail_drinks problem statement
### 1. products
This table contains the details of all the enery products.
Primary key: id

### 2. contracts
This table contains the contract details against each products
Primary key: id
Foreign key: productid

### 3. prices
This table contains the different pricing details over time against each products
Primary key: id
Foreign key: productid

### 4. contracts_prices_map
This table contains the price mapping against each contract to calculate different kpi.
Foreign key: contractid, productid

## How you use the sqlite database
The data is currently being stored in a sqlite database `enery_contract.db`. You can view the database and tables using below steps:
1. Download any DB browser App for SQLite
2. Open the database file `enery_contract.db` in the above app
3. Query any run your sql. For sample queries, please use the one from `./sql/sql_query.sql`
