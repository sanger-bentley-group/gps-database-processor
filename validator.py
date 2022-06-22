# This script takes the GPS1 database and check whether all fields in the database contain only
# the expected values or values in the expected formats for their respective columns in all tables. 

import sqlite3
import pandas as pd
import os
import sys
import logging


# Enter the current table names in the database as value under the relevant key
TABLE_NAMES = {
    'meta': 'table1_Metadata_v3',
    'qc': 'table2_QC_v3', 
    'analysis': 'table3_analysis_v3'
    }


def main():
    # Ensure the script will read and write to the same dir it locates at
    base = os.path.dirname(os.path.abspath(__file__))


    # Check file name is provided and the file exists
    try:
        db_name = sys.argv[1]
    except IndexError:
        log.critical('Invalid command. Use the following format: python validator.py database.db')
        sys.exit(1)
    
    dp_path = os.path.join(base, db_name)
    if not os.path.isfile(dp_path):
        log.critical('File does not exist')
        sys.exit(1)


    df_meta, df_qc, df_analysis = read_db(dp_path)

    check_meta(df_meta)
    check_qc(df_qc)
    check_analysis(df_analysis)

    log.info(f'The validation of {sys.argv[1]} is completed.')


def read_db(dp_path):
    try:
        dfs = []
        with sqlite3.connect(dp_path) as con:
            for table_name in TABLE_NAMES.values():
                dfs.append(pd.read_sql_query(f'SELECT * FROM {table_name}', con).astype(str))
    except pd.io.sql.DatabaseError:
        log.critical('Incorrect or incompatible database is used.')
        sys.exit(1)
    return dfs


def check_meta(df_meta):
    pass


def check_qc(df_qc):
    pass


def check_analysis(df_analysis):
    pass


# Create logger and print to the console.
def get_log():

    log = logging.getLogger("Validator")
    log.setLevel(logging.DEBUG)

    console = logging.StreamHandler()
    console.setLevel(logging.DEBUG)
    console.setFormatter(Color_Formatter())
    
    log.addHandler(console)
    return log


class Color_Formatter(logging.Formatter):
    green = "\x1b[32;20m"
    grey = "\x1b[38;20m"
    yellow = "\x1b[33;20m"
    red = "\x1b[31;20m"
    bold_red = "\x1b[31;1m"
    reset = "\x1b[0m"
    format = "%(asctime)s | %(levelname)s | %(message)s"

    FORMATS = {
        logging.DEBUG: grey + format + reset,
        logging.INFO: green + format + reset,
        logging.WARNING: yellow + format + reset,
        logging.ERROR: red + format + reset,
        logging.CRITICAL: bold_red + format + reset
    }

    def format(self, record):
        log_fmt = self.FORMATS.get(record.levelno)
        formatter = logging.Formatter(log_fmt)
        return formatter.format(record)


if __name__ == '__main__':
    log = get_log()
    main()