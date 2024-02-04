import os
from datetime import datetime

import pandas as pd
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


def transform_data(df):
    """
    To transform the extracted data
    """
    # Add calculated columns
    # df["revenue"] = df["base_price"] + (df["consumption"] * df["energy_price"])

    # Apply data historization
    df["log_time"] = pd.Timestamp.now()

    return df


def load_to_dwh(table_name, df):
    """
    Load the data into a sql darawarehouce
    """
    # # Assuming you have a SQLAlchemy engine for your database
    # your_database_engine = create_engine(
    #     "postgresql://user:password@localhost:5432/your_database"
    # )
    # # Load to Data Warehouse
    # df.to_sql(dwh_table, con=your_database_engine, if_exists="replace", index=False)
    sqlite_db.insert_dataframe(table_name=table_name, df=df)
    logger.info(f"{table_name} data inserted into DWH")


def etl_pipeline():
    """
    To design the layout of the etl pipeline
    """
    # Extract data
    source_directory = "./src_data"
    # Get file pattern for each 1st month of the year
    # file_pattern = datetime.now().strftime("%Y%m01")
    file_name_prefix = "20201101"

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

    # Transform data
    # transformed_df = transform_data(products_source_data)

    # Load data into Data Warehouse
    load_to_dwh(table_name="contracts", df=contracts_source_data)
    load_to_dwh(table_name="prices", df=prices_source_data)
    load_to_dwh(table_name="products", df=products_source_data)
    sqlite_db.close_connection()


if __name__ == "__main__":
    etl_pipeline()
