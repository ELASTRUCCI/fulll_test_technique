import sys
import logging
import datetime
from pathlib import Path, PosixPath
import pandas as pd
import sqlite3

log = logging.getLogger(__name__)


LOG_PATH = "logs"
INPUT_DATA_PATH = "data/input_data"
DB_NAME = "retail.db"


def extract_csv_file(file_path: PosixPath) -> pd.DataFrame:
    """
    Read csv file as dataframe format

    Args:
        file_path (pathlib.PosixPath)
    Returns:
        pd.DataFrame
    """
    try:
        df = pd.read_csv(file_path, sep=',', header=0)
        log.info("Extract csv file : Done")
        return df
    except Exception as e:
        sys.exit(f"Error during extract_csv_ile: {e}")


def extract_date_from_filename(file_name: str) -> str:
    """
    Return the date of transactions present in filename

    Args:
        file_name (str):

    Returns:
        str: the date
    """
    log.debug('Start extract_date_from_filename')
    day, month, year = file_name.replace('retail_', '').replace('.csv', '').split('_')
    date = year + '-' + month + '-' + day

    return date


def convert_column_type(df: pd.DataFrame) -> pd.DataFrame:
    """
    Convert column type to be ISO with transaction table

    Args:
        df (pd.DataFrame): _description_

    Returns:
        pd.DataFrame: _description_
    """
    try:
        expected_column_type = {
            'id': 'object',
            'transaction_date': 'object',
            'category': 'object',
            'name': 'object',
            'quantity': 'int64',
            'amount_excl_tax': 'float64',
            'amount_inc_tax': 'float64'
        }
        df_transformed = df.astype(expected_column_type)
        return df_transformed
    except Exception as e:
        sys.exit(f'Error during convert_column_type step: \n {e}')


def transform_data(df: pd.DataFrame, file_name: str) -> pd.DataFrame:
    """
    Process all steps needed to clean the input dataframe

    Args:
        df (pd.DataFrame): dataframe which contain all data
        file_name (str): Name of the file under study

    Returns:
        pd.DataFrame: dataframe transformed after all step
    """
    log.debug("Start transform_data")
    log.debug(f"df.shape: {df.shape}")

    #  add transaction_date
    log.debug("transaction_date")
    df.insert(1, 'transaction_date', extract_date_from_filename(file_name))
    log.debug(f"df.shape: {df.shape}")

    # Rename columns
    log.debug("Rename columns")
    df.rename(columns={'description': 'name'}, inplace=True)
    log.debug(f"df.shape: {df.shape}")

    # Remove line with null value
    log.debug("Remove Null value")
    df.dropna(axis=0, how='any', inplace=True)
    log.debug(f"df.shape: {df.shape}")

    # Remove duplicate.
    # I consider that if duplicate can occur, i will keep the recent one, the last of the df
    log.debug("Remove duplicate ")
    df.drop_duplicates(subset='id', keep='last', inplace=True)
    log.debug(f"df.shape: {df.shape}")

    # Convert columns type
    log.debug("Remove duplicate ")
    df_transformed = convert_column_type(df)
    log.debug(f"df.shape: {df.shape}")

    # Change to id Index
    df_transformed = df_transformed.set_index('id')

    log.info("Data transformation step: Done")

    return df_transformed


def load_to_db(df: pd.DataFrame) -> None:
    """
    Insert into db information from input dataframe
    There is not primary key on the table, we can't perform an UPSERT strategy.
    We will perform a deletion step + insertion step

    Args:
        df (pd.DataFrame): Input dataframe which contain transactions
    """

    log.debug("Start load_to_db")
    try:
        conn = sqlite3.connect(DB_NAME)
        log.info('DB connected')

        cur = conn.cursor()

        db_size = cur.execute("SELECT COUNT(*) FROM transactions").fetchone()[0]
        log.info(f'Initial table size: {db_size}')

        # delete already exising ID on the DB
        log.debug('Delete already existing ID')
        id_to_delete = tuple(df.index.to_list())
        cur.execute(f"DELETE FROM transactions WHERE id IN {id_to_delete}")
        nb_changes = cur.execute("SELECT changes()").fetchone()[0]
        log.info(f'Number of line deleted cause ID already exist : {nb_changes}')

        # Insert data from the df
        df.to_sql('transactions', conn, if_exists="append")
        cur.execute("SELECT changes()").fetchone()
        db_size = cur.execute("SELECT COUNT(*) FROM transactions").fetchone()[0]
        log.info(f'Table size after insertion: {db_size}')

        conn.commit()
        log.info('Change have been commited')
    except Exception as error:
        log.info(f"Error occured during load_to_db: \n{error}")
    finally:
        if conn:
            conn.close()
            log.info("DB connection is closed")


def main():
    """
    This function will perform the basic steps of an ETL pipeline:
    Extract, Transform, Load into sqlite database.
    """
    now = datetime.datetime.now(datetime.UTC)
    dt_string = now.strftime("%Y_%m_%d_%H_%M_%S")

    log_file_name = f"{LOG_PATH}/log_etl_{dt_string}.log"
    log_file_path = Path(log_file_name)
    log_file_path.parent.mkdir(parents=True, exist_ok=True)

    log_format = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    logging.basicConfig(filename=log_file_path,
                        format=log_format,
                        level=logging.INFO)

    log.info(f"INPUT DATA FOLDER: {INPUT_DATA_PATH}")
    log.info(f"DATABASE IN USE: {DB_NAME}")

    current_dir = Path.cwd()
    files_path_to_process = [p for p in current_dir.glob(
        f'{INPUT_DATA_PATH}/retail_*.csv') if p.is_file()]

    log.info(f"Number of file to process : {len(files_path_to_process)}")

    if len(files_path_to_process) == 0:
        log.info('THE ETL PROCESS IS DONE')
        log.warning(f'NO FILE FOUND AT {current_dir}/{INPUT_DATA_PATH}")')
        return False

    # for each file present in the input directory
    for file_path in files_path_to_process:
        log.info('---------')
        log.info(f"file under process : {file_path}")

        df_data = extract_csv_file(file_path)

        if not df_data.empty:
            df_transformed_data = transform_data(df_data, file_path.name)
            log.info(f'Nb of lines to insert: {len(df_transformed_data)}')
            load_to_db(df_transformed_data)
        else:
            log.info("Empty input file. Nothing else will be done ")

        log.info(f"Done for the file : {file_path}")


if __name__ == "__main__":
    main()
