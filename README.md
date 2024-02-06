# Enery Contract Pricing Model
This is an Enery Contract Pricing Model use case project for Data Engineering. You can the three files from the surce which are recieved at the 1st of each month around 10 p.m. So you have to design and schedule a pipeline that process the recieved datsets.

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

## How to schedule this job
Since the files are recieved every first day of the month around 10, the etl job will be scheduled every 1st day of the month at 11 pm.
```
Cron expression: 0 23 1 * * 
```
1. You can use any schedulers like Airflow to schedule
2. For codes deployed on Linux/Unix, can also setup cron jobs. Steps include:
    - In your terminal, type `crontab -e` to edit your cron jobs.
    - Add the following line at the end of the file and replace the path to the python and your .py file for this project:
    ```
    0 23 1 * * /path/to/python /path/to/energy-contract-price/enery_contract.py
    ```
3. You can also use other ways to schedule this job

## Folder structure for this project
1. `./log/`: Location to store the log files
2. `./sql/`: Location to store all the sql queries that might be used in future
3. `./sql/sql_query.sql`: Contains sample sql queries against few business use cases
4. `./src_data/`: Location where source data arrives at the start of every month
5. `./util/logger.py`: Common library designed for the purpose of logging
6. `./util/sqlite_conn.py`: Python DWH utility that uses sqlite database to store the files
7. `energy_contract.db`: SQLite Database file storing all the tables structure and data for this project
8. `requirements.txt`: A file containing all the python libraries used in this project that needs to be installed. Please check and keep this updated inorder to install all the libraries used at once using the command `pip install -r requirements.txt`

## Tables structure for the cocktail_drinks problem statement
### 1. products
This table contains the details of all the enery products and its type with other details.
```
Primary key: id
```

### 2. contracts
This table contains the contract details against each products. It contains information like the usage of the enery per year, status, contract registered on, start and end date of the contract, city and other details
```
Primary key: id
Foreign key: productid
```

### 3. prices
This table contains the different pricing details over time against each products
```
Primary key: id
Foreign key: productid
```

### 4. contracts_products_prices_map
This is the final table that contains the price mapping for each contract against a product/enrergy. It get me join with the other three raw tables to get all the relevant information regarding contracts, products or its useage and pricing details. This is the main table shared for the business anlytics purpose, eg: to calculate revenue or kpi. You can keep on adding more columns to make it more specific for the various business needs.
```
Foreign key: contractid, productid, id_baseprice, id_workingprice
```

## How you use the sqlite database
The data is currently being stored in a sqlite database `enery_contract.db`. You can view the database and tables using below steps:
1. Download any DB browser App for SQLite
2. Open the database file `enery_contract.db` in the above app
3. Query any run your sql. For sample queries, please use the one from `./sql/sql_query.sql`

## Assumptions
1. Since we recieve a full refresh of the entire data every month, not storing the backup or history
2. The usage price of the contract depends when the contract was created and reamin fixed for the entire duration of the contract. This is helful in calculating the revenue based on its usage
3. Contract On delivery means the time between the start date and end date of a contracts when they were actually running
4. The load date of any contract is the date the contract is loaded to the DWH. 
E.g. A customer could sign a contract today with start date in May 2024. 
It would be loaded to the DWH then tonight, but it is not on delivery until May.
For new contract loaded on May, it should be created within last month ie: April
So the no. of contracts loaded into DWH on Dec 2020 will be any contracts created in the month of Nov 2020
5. If a product mapped to a contract is not available in the products imports recieved, this means that we don't be considereing the contract.
6. If the priceing details for a product for a specific time is not available, this means we wont be able to calculate the total price/revenue ffrom that contract.

## Future Enhancement
1. If required to store the data recieved every month for historical reasons, we can store the files in a Data Lake eg: S3 with a retention policy based on the active and passive usage to save the cost
2. If the priceing details for a product for a specific time is not available, this means we wont be able to calculate the total price/revenue from that contract.
--> To handle this case, we can assume the very first entry of the pricing to be the base starting pricing model to consider for a product.
