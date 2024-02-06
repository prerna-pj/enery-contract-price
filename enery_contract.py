import os
import time
from datetime import datetime

import pandas as pd
import schedule
from sqlalchemy import create_engine

from util.logger import setup_logger
from util.sqlite_conn import SQLiteConnection

# create a logger instance
logger = setup_logger()

SQLITE_DB_NAME = "enery_contract.db"
# create a sqlite db connection
sqlite_db = SQLiteConnection(db_name=SQLITE_DB_NAME)
sqlite_db.connect()


def extract_data(
    folder_directory: str, file_name_prefix: str, file_name_suffix: str, delimiter: str
) -> pd.DataFrame:
    """
    Extract data from csv files
    - param folder_directory:Directory path for the file location
    - param file_name_prefix: file name prefix
    - param file_name_suffix: file name suffix
    - param delimiter: Delimiter for the csv file
    - return df: pandas dataframe
    """
    for file in os.listdir(folder_directory):
        if file.startswith(file_name_prefix) and file.endswith(file_name_suffix):
            monthly_file = file

    df = pd.read_csv(os.path.join(folder_directory, monthly_file), delimiter=delimiter)
    return df


def transform_data(df_contracts: pd.DataFrame, df_prices: pd.DataFrame) -> pd.DataFrame:
    """
    To create a cotracts and pricing mapping model
    Parameters:
    - df_contracts: Dataframe containing imported contracts data
    - df_prices: Dataframe containing imported prices data per enery product
    Return:
    contracts_products_prices_map: Returns a pandas Dataframe containing the price mapping against each contract
    """
    # Merge the DataFrames based on the specified conditions
    contracts_products_prices_map = pd.merge(
        df_contracts[["id", "productid", "createdat", "usage", "startdate"]],
        df_prices[
            [
                "id",
                "productid",
                "productcomponent",
                "price",
                "unit",
                "valid_from",
                "valid_until",
            ]
        ],
        on=["productid"],
        how="inner",
        suffixes=("_contracts", "_prices"),
    ).query("valid_from <= createdat <= valid_until")[
        [
            "id_contracts",
            "id_prices",
            "productid",
            "usage",
            "productcomponent",
            "price",
            "unit",
        ]
    ]

    # Pivot the DataFrame to have unique 'id' values as rows and 'price' and 'unit' attribute values as columns
    contracts_products_prices_map = contracts_products_prices_map.pivot_table(
        index=["id_contracts", "productid", "usage"],
        columns="productcomponent",
        values=["id_prices", "price", "unit"],
        aggfunc="first",
    )

    # Flatten MultiIndex columns
    contracts_products_prices_map.columns = [
        "_".join(map(str, col)).strip()
        for col in contracts_products_prices_map.columns.values
    ]

    # Resetting index and enaming columns and for a cleaner DataFrame
    contracts_products_prices_map.reset_index(inplace=True)
    contracts_products_prices_map = contracts_products_prices_map.rename(
        columns={
            "id_contracts": "contractid",
            "id_prices_baseprice": "id_baseprice",
            "id_prices_workingprice": "id_workingprice",
        }
    )

    # Add the DWH load time for the contracts enery prices model
    contracts_products_prices_map["load_time"] = pd.Timestamp.now()

    return contracts_products_prices_map


def data_validation():
    """
    Validate the data
    """
    pass


def load_to_dwh(table_name: str, df: pd.DataFrame, insert_type: str) -> None:
    """
    Load the data into a Sqlite darawarehouce
    Parameters:
    - tablename: Name of the table where data needs to be inserted
    - df: Pandas dataframe that needs to be inserted into the DWH
    - insert_type: Behavior of insert if the table already exists. ['fail', 'replace', 'append']
    """
    sqlite_db.insert_dataframe(table_name=table_name, df=df, insert_type=insert_type)
    logger.info(f"{table_name} data inserted into DWH")


def etl_pipeline():
    """
    To design the layout of the etl pipeline
    """
    # Extract data
    source_directory = "./src_data"
    # Get file pattern for each 1st month of the year
    file_name_prefix = datetime.now().strftime("%Y%m01")

    products_source_data = extract_data(
        folder_directory=source_directory,
        file_name_prefix=file_name_prefix,
        file_name_suffix="_products.csv",
        delimiter=";",
    )
    prices_source_data = extract_data(
        folder_directory=source_directory,
        file_name_prefix=file_name_prefix,
        file_name_suffix="_prices.csv",
        delimiter=";",
    )
    contracts_source_data = extract_data(
        folder_directory=source_directory,
        file_name_prefix=file_name_prefix,
        file_name_suffix="_contracts.csv",
        delimiter=";",
    )

    # Transform and Validate the data
    contracts_products_prices_map = transform_data(
        df_contracts=contracts_source_data, df_prices=prices_source_data
    )

    # Load data into Data Warehouse
    load_to_dwh(table_name="contracts", df=contracts_source_data, insert_type="replace")
    load_to_dwh(table_name="prices", df=prices_source_data, insert_type="replace")
    load_to_dwh(table_name="products", df=products_source_data, insert_type="replace")
    load_to_dwh(
        table_name="contracts_products_prices_map",
        df=contracts_products_prices_map,
        insert_type="replace",
    )
    sqlite_db.close_connection()


if __name__ == "__main__":

    etl_pipeline()
