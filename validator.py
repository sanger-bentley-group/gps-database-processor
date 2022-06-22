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

# Enter the column names of each table in list
META_COLUMNS = ["Sample_name", "Public_name", "Study_name", "Selection_random", "Continent", "Country", "Region", "City", "Facility_where_collected", "Submitting_institution", "Month_collection", "Year_collection", "Gender", "Age_years", "Age_months", "Age_days", "Clinical_manifestation", "Source", "HIV_status", "Underlying_conditions", "Phenotypic_serotype_method", "Phenotypic_serotype", "Sequence_Type", "aroE", "gdh", "gki", "recP", "spi", "xpt", "ddl", "AST_method_Penicillin", "Penicillin", "AST_method_Amoxicillin", "Amoxicillin", "AST_method_Cefotaxime", "Cefotaxime", "AST_method_Ceftriaxone", "Ceftriaxone", "AST_method_Cefuroxime", "Cefuroxime", "AST_method_Meropenem", "Meropenem", "AST_method_Erythromycin", "Erythromycin", "AST_method_Clindamycin", "Clindamycin", "AST_method_Trim/Sulfa", "Trim/Sulfa", "AST_method_Vancomycin", "Vancomycin", "AST_method_Linezolid", "Linezolid", "AST_method_Ciprofloxacin", "Ciprofloxacin", "AST_method_Chloramphenicol", "Chloramphenicol", "AST_method_Tetracycline", "Tetracycline", "AST_method_Levofloxacin", "Levofloxacin", "AST_method_Synercid", "Synercid", "AST_method_Rifampin", "Rifampin", "Comments", "Latitude", "Longitude", "Resolution", "Vaccine_period", "Introduction_year", "PCV_type"]
QC_COLUMNS = ["Lane_id", "Streptococcus_pneumoniae", "Total_length", "No_of_contigs", "Genome_covered", "Depth_of_coverage", "Proportion_of_Het_SNPs", "QC", "Supplier_name", "Hetsites_50bp"]
ANALYSIS_COLUMNS = ["Lane_id", "Sample", "Public_name", "ERR", "ERS", "No_of_genome", "Duplicate", "Paper_1", "In_silico_ST", "aroE", "gdh", "gki", "recP", "spi", "xpt", "ddl", "Country", "Continent", "Manifest_type", "Children<5yrs", "GPSC", "GPSC__colour", "In_silico_serotype", "In_silico_serotype__colour", "pbp1a", "pbp2b", "pbp2x", "WGS_PEN", "WGS_PEN_SIR_Meningitis", "WGS_PEN_SIR_Nonmeningitis", "WGS_AMO", "WGS_AMO_SIR", "WGS_MER", "WGS_MER_SIR", "WGS_TAX", "WGS_TAX_SIR_Meningitis", "WGS_TAX_SIR_Nonmeningitis", "WGS_CFT", "WGS_CFT_SIR_Meningitis", "WGS_CFT_SIR_Nonmeningitis", "WGS_CFX", "WGS_CFX_SIR", "WGS_ERY", "WGS_ERY_SIR", "WGS_CLI", "WGS_CLI_SIR", "WGS_SYN", "WGS_SYN_SIR", "WGS_LZO", "WGS_LZO_SIR", "WGS_ERY_CLI", "WGS_COT", "WGS_COT_SIR", "WGS_TET", "WGS_TET_SIR", "WGS_DOX", "WGS_DOX_SIR", "WGS_LFX", "WGS_LFX_SIR", "WGS_CHL", "WGS_CHL_SIR", "WGS_RIF", "WGS_RIF_SIR", "WGS_VAN", "WGS_VAN_SIR", "EC", "Cot", "Tet__autocolour", "FQ__autocolour", "Other", "PBP1A_2B_2X__autocolour", "WGS_PEN_SIR_Meningitis__colour", "WGS_PEN_SIR_Nonmeningitis__colour", "WGS_AMO_SIR__colour", "WGS_MER_SIR__colour", "WGS_TAX_SIR_Meningitis__colour", "WGS_TAX_SIR_Nonmeningitis__colour", "WGS_CFT_SIR_Meningitis__colour", "WGS_CFT_SIR_Nonmeningitis__colour", "WGS_CFX_SIR__colour", "WGS_ERY_SIR__colour", "WGS_CLI_SIR__colour", "WGS_SYN_SIR__colour", "WGS_LZO_SIR__colour", "WGS_COT_SIR__colour", "WGS_TET_SIR__colour", "WGS_DOX_SIR__colour", "WGS_LFX_SIR__colour", "WGS_CHL_SIR__colour", "WGS_RIF_SIR__colour", "WGS_VAN_SIR__colour", "ermB", "ermB__colour", "mefA", "mefA__colour", "folA_I100L", "folA_I100L__colour", "folP__autocolour", "cat", "cat__colour", "PCV7", "PCV10", "PCV13", "PCV15", "PCV20", "Pneumosil", "Published(Y/N)"]


def main():
    global log 
    log = get_log()

    # Ensure the script will read and write to the same dir it locates at
    base = os.path.dirname(os.path.abspath(__file__))

    # Check file name is provided and the file exists
    try:
        db_name = sys.argv[1]
    except IndexError:
        log.critical('Invalid command. Use the following format: python validator.py database.db')
        sys.exit(1)
    
    global dp_path
    dp_path = os.path.join(base, db_name)
    if not os.path.isfile(dp_path):
        log.critical('File does not exist')
        sys.exit(1)

    df_meta, df_qc, df_analysis = read_db()

    check_meta(df_meta)
    check_qc(df_qc)
    check_analysis(df_analysis)

    log.info(f'The validation of {sys.argv[1]} is completed.')


# Read database into Pandas dataframes for processing
def read_db():
    try:
        dfs = []
        with sqlite3.connect(dp_path) as con:
            for table_name in TABLE_NAMES.values():
                dfs.append(pd.read_sql_query(f'SELECT * FROM {table_name}', con).astype(str))
    except pd.io.sql.DatabaseError:
        log.critical('Unable to find all tables in the database. Incorrect or incompatible database is used.')
        sys.exit(1)
    return dfs


# Check whether meta table only contains expected values / patterns
def check_meta(df_meta):
    table_name = TABLE_NAMES["meta"]

    check_columns(df_meta, META_COLUMNS, table_name)
    check_y_n(df_meta, 'Selection_random', table_name)

# Check whether qc table only contains expected values / patterns
def check_qc(df_qc):
    check_columns(df_qc, QC_COLUMNS, TABLE_NAMES["qc"])


# Check whether analysis table only contains expected values / patterns
def check_analysis(df_analysis):
    check_columns(df_analysis, ANALYSIS_COLUMNS, TABLE_NAMES["analysis"])


def check_columns(df, columns, table_name):
    if (diff := set(list(df)) - set(columns)):
        log.critical(f'{table_name} has the following unexpected column(s): {", ".join(diff)}. Incorrect or incompatible database is used.')
        sys.exit(1)
    if (diff := set(list(columns)) - set(df)):
        log.critical(f'{table_name} is missing the following column(s): {", ".join(diff)}. Incorrect or incompatible database is used.')
        sys.exit(1)


# Check column values contain Y, N, _ only
def check_y_n(df, column_name, table_name):
    expected = {'Y', 'N', '_'}
    check_expected(df, column_name, table_name, expected)


# Check column values against expected values; correct lowercase string is there is any; report unexpected value(s) if there is any 
def check_expected(df, column_name, table_name, expected):
    extras = set(df[column_name].unique()) - expected
    if extras == set():
        return

    casefolded_extras = set(x.casefold() for x in df[column_name].unique()) - {x.casefold() for x in expected}
    if casefolded_extras == set():
        db_update_to_upper(table_name, column_name)
        log.warning(f'{column_name} in {table_name} contained lowercase version of the expected values. They are now corrected.')
    else:
        log.error(f'{column_name} in {table_name} has the following unexpected value(s): {", ".join(extras)}.')


# Modify the database, correct all strings to uppercase in the selected column
def db_update_to_upper(table_name, column_name):
    with sqlite3.connect(dp_path) as con:
        cur = con.cursor()
        cur.execute(f'''
                    UPDATE {table_name}
                    SET {column_name} = UPPER({column_name})
                    ''')
        con.commit()


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
    main()