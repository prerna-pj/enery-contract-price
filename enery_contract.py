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

# def get_filename(folder_directory: str, file_name_prefix: str, file_name_suffix: str) -> str:
#     """
#      Extract data from csv files
#     - param folder_directory: Directory path for the file location
#     - param file_name_prefix: file name prefix
#     - param file_name_suffix: file name suffix
#     - return filename: Name of the file
#     """
#     for filename in os.listdir(folder_directory):
#         if filename.startswith(file_name_prefix) and filename.endswith(file_name_suffix):
#             return filename


def create_sqlite_tables() -> None:
    """
    Create tables on sqlite using sql query
    """
    try:
        with open("sql/create_tab_cocktails_details.sql", "r") as sql_file:
            sql_cocktails_details = sql_file.read()
        with open("sql/create_tab_cocktails_ingredients.sql", "r") as sql_file:
            sql_cocktails_ingredients = sql_file.read()

        sqlite_db.create_table(create_table_query=sql_cocktails_details)
        sqlite_db.create_table(create_table_query=sql_cocktails_ingredients)
    except Exception as e:
        logger.exception(f"Error in reading the sql files: {e}")


def extract_data(
    folder_directory: str, file_name_prefix: str, file_name_suffix: str, delimiter: str
):
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
    # print(df)
    return df


def transform_data(df_contracts: pd.DataFrame, df_prices: pd.DataFrame) -> pd.DataFrame:
    """
    To create a cotracts and pricing mapping model
    Parameters:
    - df_contracts: Dataframe containing imported contracts data
    - df_prices: Dataframe containing imported prices data per enery product
    Return:
    contracts_price_map: Returns a pandas Dataframe containing the price mapping against each contract
    """
    # Merge the DataFrames based on the specified conditions
    contracts_price_map = pd.merge(
        df_contracts[["id", "productid", "createdat", "usage", "startdate"]],
        df_prices[
            [
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
            "id",
            "productid",
            "usage",
            "productcomponent",
            "price",
            "unit",
        ]
    ]

    # Pivot the DataFrame to have unique 'id' values as rows and 'price' and 'unit' attribute values as columns
    contracts_price_map = contracts_price_map.pivot_table(
        index=["id", "productid"],
        columns="productcomponent",
        values=["price", "unit"],
        aggfunc="first",
    )

    # Flatten MultiIndex columns
    contracts_price_map.columns = [
        "_".join(map(str, col)).strip() for col in contracts_price_map.columns.values
    ]

    # Resetting index and enaming columns and for a cleaner DataFrame
    contracts_price_map.reset_index(inplace=True)
    contracts_price_map = contracts_price_map.rename(columns={"id": "contractid"})

    return contracts_price_map


def add_historization(df, load_date):
    """
    Add load date as per the data imports data
    """
    date_timestamp = pd.to_datetime(load_date, format="%Y%m%d")
    df["load_time"] = date_timestamp
    return df


def data_validation():
    """
    Validate the data
    """
    pass


def load_to_dwh(table_name, df, insert_type):
    """
    Load the data into a sql darawarehouce
    """
    # # Assuming you have a SQLAlchemy engine for your database
    # your_database_engine = create_engine(
    #     "postgresql://user:password@localhost:5432/your_database"
    # )
    # # Load to Data Warehouse
    # df.to_sql(dwh_table, con=your_database_engine, if_exists="replace", index=False)
    print(f"Inserting data into table")
    sqlite_db.insert_dataframe(table_name=table_name, df=df, insert_type=insert_type)
    logger.info(f"{table_name} data inserted into DWH")


def etl_pipeline():
    """
    To design the layout of the etl pipeline
    """
    # Extract data
    source_directory = "./src_data"
    # Get file pattern for each 1st month of the year
    # file_pattern = datetime.now().strftime("%Y%m01")
    file_name_prefix = "20210101"

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

    # Validate the data

    # Transform data
    contracts_prices_map = transform_data(
        df_contracts=contracts_source_data, df_prices=prices_source_data
    )

    # Add historization to the contracts details as it can have mutiple records if the contracts gets updated
    contracts_source_data = add_historization(
        df=contracts_source_data, load_date=file_name_prefix
    )

    # Load data into Data Warehouse
    load_to_dwh(table_name="contracts", df=contracts_source_data, insert_type="replace")
    load_to_dwh(table_name="prices", df=prices_source_data, insert_type="replace")
    load_to_dwh(table_name="products", df=products_source_data, insert_type="replace")
    load_to_dwh(
        table_name="contracts_prices_map",
        df=contracts_prices_map,
        insert_type="replace",
    )
    sqlite_db.close_connection()


if __name__ == "__main__":

    etl_pipeline()

    # # Schedule the data pipeline to run at 11 PM on the 1st of each month
    # schedule.every().month.at("23:00").day.at("01:00").do(etl_pipeline)

    # # Infinite loop to keep the script running
    # while True:
    #     schedule.run_pending()
    #     time.sleep(1)  # Sleep for 1 second to avoid high CPU usage
