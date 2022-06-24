# This script takes the GPS1 database and check whether all fields in the database contain only
# the expected values or values in the expected formats for their respective columns in all tables. 

import sqlite3
import pandas as pd
import os
import sys
import re
from datetime import date
from colorlog import get_log


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


# Global variables 
LOG = get_log()
FOUND_ERRORS = False


def main():
    # Ensure the script will read and write to the same dir it locates at
    base = os.path.dirname(os.path.abspath(__file__))

    # Check file name is provided and the file exists
    try:
        db_name = sys.argv[1]
    except IndexError:
        LOG.critical('Invalid command. Use the following format: python validator.py database.db')
        sys.exit(1)
    
    dp_path = os.path.join(base, db_name)
    if not os.path.isfile(dp_path):
        LOG.critical('File does not exist')
        sys.exit(1)

    df_meta, df_qc, df_analysis = read_db(dp_path)

    check_meta(df_meta, dp_path)
    check_qc(df_qc, dp_path)
    check_analysis(df_analysis, dp_path)

    if FOUND_ERRORS:
        LOG.error(f'The validation of {sys.argv[1]} is completed with error(s).')
    else:
        LOG.info(f'The validation of {sys.argv[1]} is completed.')


# Read database into Pandas dataframes for processing
def read_db(dp_path):
    try:
        dfs = []
        with sqlite3.connect(dp_path) as con:
            for table_name in TABLE_NAMES.values():
                dfs.append(pd.read_sql_query(f'SELECT * FROM {table_name}', con).astype(str))
    except pd.io.sql.DatabaseError:
        LOG.critical('Unable to find all tables in the database. Incorrect or incompatible database is used.')
        sys.exit(1)
    return dfs


# Check whether meta table only contains expected values / patterns
def check_meta(df_meta, dp_path):
    table_name = TABLE_NAMES["meta"]

    check_columns(df_meta, META_COLUMNS, table_name)
    check_y_n(df_meta, 'Selection_random', table_name, dp_path)
    check_continent(df_meta, 'Continent', table_name, dp_path)
    check_country(df_meta, 'Country', table_name, dp_path)
    check_month(df_meta, 'Month_collection', table_name, dp_path)
    check_year(df_meta, 'Year_collection', table_name)
    check_gender(df_meta, 'Gender', table_name, dp_path)
    check_age_years(df_meta, 'Age_years', table_name)
    check_age_months(df_meta, 'Age_months', table_name)
    check_age_days(df_meta, 'Age_days', table_name)
    check_p_n(df_meta, 'HIV_status', table_name, dp_path)
    check_phenotypic_serotype(df_meta, 'Phenotypic_serotype', table_name, dp_path)
    check_sequence_type(df_meta, 'Sequence_Type', table_name, dp_path)

    mlst_genes_columns = ['aroE', 'gdh', 'gki', 'recP', 'spi', 'xpt', 'ddl']
    for col in mlst_genes_columns:
        check_mlst_gene(df_meta, col, table_name, dp_path)

    antibiotics_columns = ['Penicillin', 'Amoxicillin', 'Cefotaxime', 'Ceftriaxone', 'Cefuroxime', 'Meropenem', 'Erythromycin', 'Clindamycin', 'Trim/Sulfa', 'Vancomycin', 'Linezolid', 'Ciprofloxacin', 'Chloramphenicol', 'Tetracycline', 'Levofloxacin', 'Synercid', 'Rifampin']
    for col in antibiotics_columns:
        check_antibiotic_ast(df_meta, col, table_name, dp_path)

    check_case_only_columns = ['Sample_name', 'Public_name', 'Study_name', 'Region', 'City', 'Facility_where_collected', 'Submitting_institution', 'Clinical_manifestation', 'Source', 'Underlying_conditions', 'Phenotypic_serotype_method', 'AST_method_Penicillin', 'AST_method_Amoxicillin', 'AST_method_Cefotaxime', 'AST_method_Ceftriaxone', 'AST_method_Cefuroxime', 'AST_method_Meropenem', 'AST_method_Erythromycin', 'AST_method_Clindamycin', 'AST_method_Trim/Sulfa', 'AST_method_Vancomycin', 'AST_method_Linezolid', 'AST_method_Ciprofloxacin', 'AST_method_Chloramphenicol', 'AST_method_Tetracycline', 'AST_method_Levofloxacin', 'AST_method_Synercid', 'AST_method_Rifampin']
    for col in check_case_only_columns:
        check_case(df_meta, col, table_name, dp_path)


# Check whether qc table only contains expected values / patterns
def check_qc(df_qc, dp_path):
    table_name = TABLE_NAMES["qc"]

    check_columns(df_qc, QC_COLUMNS, table_name)


# Check whether analysis table only contains expected values / patterns
def check_analysis(df_analysis, dp_path):
    table_name = TABLE_NAMES["analysis"]

    check_columns(df_analysis, ANALYSIS_COLUMNS, table_name)


# Check whether tables contain only the expected columns
def check_columns(df, columns, table_name):
    if (diff := set(list(df)) - set(columns)):
        LOG.critical(f'{table_name} has the following unexpected column(s): {", ".join(diff)}. Incorrect or incompatible database is used.')
        sys.exit(1)
    if (diff := set(list(columns)) - set(df)):
        LOG.critical(f'{table_name} is missing the following column(s): {", ".join(diff)}. Incorrect or incompatible database is used.')
        sys.exit(1)


# Check column values contain Y, N, _ only
def check_y_n(df, column_name, table_name, dp_path):
    expected = {'Y', 'N', '_'}
    check_expected(df, column_name, table_name, expected, dp_path)


# Check column values is in the expected continent only
def check_continent(df, column_name, table_name, dp_path):
    expected = {'AFRICA', 'ASIA', 'CENTRAL AMERICA', 'EUROPE', 'LATIN AMERICA', 'NORTH AMERICA', 'OCEANIA', '_'}
    check_expected(df, column_name, table_name, expected, dp_path)


# Warn if column values contain previously unknown countries
def check_country(df, column_name, table_name, dp_path):
    expected = {'ARGENTINA', 'BANGLADESH', 'BELARUS', 'BENIN', 'BOTSWANA', 'BRAZIL', 'BULGARIA', 'CAMBODIA', 'CAMEROON', 'CANADA', 'CENTRAL AFRICAN REPUBLIC', 'CHINA', 'CROATIA', 'CZECH REPUBLIC', 'DRC CONGO', 'ECUADOR', 'EGYPT', 'ETHIOPIA', 'FRANCE', 'GHANA', 'GUATEMALA', 'HUNGARY', 'INDIA', 'INDONESIA', 'IRELAND', 'ISRAEL', 'IVORY COAST', 'KENYA', 'KUWAIT', 'LATVIA', 'LITHUANIA', 'MALAWI', 'MALAYSIA', 'MONGOLIA', 'MOROCCO', 'MOZAMBIQUE', 'NEPAL', 'NETHERLANDS', 'NEW ZEALAND', 'NIGER', 'NIGERIA', 'OMAN', 'PAKISTAN', 'PAPUA NEW GUINEA', 'PERU', 'POLAND', 'QATAR', 'RUSSIAN FEDERATION', 'SENEGAL', 'SLOVENIA', 'SOUTH AFRICA', 'SPAIN', 'SWEDEN', 'TAIWAN', 'THAILAND', 'THE GAMBIA', 'TOGO', 'TRINIDAD AND TOBAGO', 'TURKEY', 'USA', 'WEST AFRICA', '_'}
    check_expected(df, column_name, table_name, expected, dp_path, absolute=False)


# Check column values is in the expected months only
def check_month(df, column_name, table_name, dp_path):
    expected = {'JAN', 'FEB', 'MAR', 'APR', 'MAY', 'JUN', 'JUL', 'AUG', 'SEP', 'OCT', 'NOV', 'DEC', '_'}
    check_expected(df, column_name, table_name, expected, dp_path)


# Check column values is within reasonable year range
def check_year(df, column_name, table_name):
    uniques_non_empty = get_uniques_non_empty(df, column_name)
    unexpected = [unique for unique in uniques_non_empty if not unique.isdecimal() or not 1985 <= int(unique) <= date.today().year]
    
    if len(unexpected) == 0:
        return

    LOG.error(f'{column_name} in {table_name} has the following unexpected value(s): {", ".join(unexpected)}.')
    found_error()


# Check column values is in the expected genders only
def check_gender(df, column_name, table_name, dp_path):
    expected = {'M', 'F', '_'}
    check_expected(df, column_name, table_name, expected, dp_path)


# Check column values is within reasonable age year 
def check_age_years(df, column_name, table_name):
    uniques_non_empty = get_uniques_non_empty(df, column_name)
    unexpected = [unique for unique in uniques_non_empty if not re.match(r'^([0-9]*[.])?[0-9]+$', unique) or not 0 <= float(unique) <= 130]
    
    if len(unexpected) == 0:
        return
    
    check_no_alphabet_only_numeric(unexpected, column_name, table_name)


# Check column values is within reasonable age month 
def check_age_months(df, column_name, table_name):
    uniques_non_empty = get_uniques_non_empty(df, column_name)
    unexpected = [unique for unique in uniques_non_empty if not re.match(r'^([0-9]*[.])?[0-9]+$', unique) or not 0 <= float(unique) <= 12]
    
    if len(unexpected) == 0:
        return
    
    check_no_alphabet_only_numeric(unexpected, column_name, table_name)


# Check column values contain 0 - 31 integers or _ only 
def check_age_days(df, column_name, table_name):
    uniques_non_empty = get_uniques_non_empty(df, column_name)
    unexpected = [unique for unique in uniques_non_empty if not unique.isdecimal() or not 0 <= int(unique) <= 31]
    
    if len(unexpected) == 0:
        return
    
    LOG.error(f'{column_name} in {table_name} has the following unexpected value(s): {", ".join(unexpected)}.')
    found_error()


# Check column values contain P, N, _ only
def check_p_n(df, column_name, table_name, dp_path):
    expected = {'P', 'N', '_'}
    check_expected(df, column_name, table_name, expected, dp_path)


# Check column values fit the phenotypic serotype pattern 
# Expect "NT" or "serotype, optionally followed by [separated by / or &] serotype or sub-group or NT"
def check_phenotypic_serotype(df, column_name, table_name, dp_path):
    check_case(df, column_name, table_name, dp_path)
    uniques_non_empty = get_uniques_non_empty(df, column_name)
    unexpected = [unique for unique in uniques_non_empty if not re.match(r'^(NT|([1-9][0-9]*[A-Z]?)((&|\/)(?!$)(([1-9][0-9]*)*[A-Z]?|NT))*)$', unique)]
        
    if len(unexpected) == 0:
        return
    
    LOG.warning(f'{column_name} in {table_name} has the following non-standard phenotypic serotype value(s): {", ".join(unexpected)}. Please check if they are correct.')


# Check column values contain UNKNOWN or 1 - 50000 integers or _ only
def check_sequence_type(df, column_name, table_name, dp_path):
    check_range_with_unknowns(df, column_name, table_name, dp_path, lo=1, hi=50000)


# Check column values contain UNKNOWN or 1 - 1000 integers or _ only
def check_mlst_gene(df, column_name, table_name, dp_path):
    check_range_with_unknowns(df, column_name, table_name, dp_path, lo=1, hi=1000)


# Check column values contain numeric values (can be a range: >, <, >=, <=) or I or R or S or _ only 
def check_antibiotic_ast(df, column_name, table_name, dp_path):
    check_case(df, column_name, table_name, dp_path)
    uniques_non_empty = get_uniques_non_empty(df, column_name)
    unexpected = [unique for unique in uniques_non_empty if not re.match(r'^([IRS0]|([<>]=?)?((0\.0*)?[1-9][0-9]*(\.[0-9]*[1-9])?))$', unique)]
        
    if len(unexpected) == 0:
        return
    
    LOG.error(f'{column_name} in {table_name} has the following unexpected value(s): {", ".join(unexpected)}.')
    found_error()


# Check column values against expected values; correct lowercase string is there is any; report unexpected value(s) if there is any 
def check_expected(df, column_name, table_name, expected, dp_path, absolute=True):
    check_case(df, column_name, table_name, dp_path)
    extras = set(df[column_name].unique()) - expected
    
    if len(extras) == 0:
        return
    
    if absolute:
        LOG.error(f'{column_name} in {table_name} has the following unexpected value(s): {", ".join(extras)}.')
        found_error()
    else:
        LOG.warning(f'{column_name} in {table_name} has the following previously unknown value(s): {", ".join(extras)}. Please check if they are correct.')


# Check whether all strings are uppercase in the selected column, ignore values without alphabets; convert all strings to upper if any lowercase found
def check_case(df, column_name, table_name, dp_path):
    uniques_with_alphabets = (unique for unique in df[column_name].unique() if re.search(r'[a-zA-Z]', unique))
    
    if all(unique.isupper() for unique in uniques_with_alphabets):
        return

    df[column_name] = df[column_name].str.upper()
    db_update_to_upper(table_name, column_name, dp_path)
    LOG.warning(f'{column_name} in {table_name} contained lowercase value(s). They are now corrected.')


# Get uniques values in a column, excluding '_'
def get_uniques_non_empty(df, column_name):
    return [unique for unique in df[column_name].unique() if unique != '_']


# Check unexpected only contains unexpected values without alphabet, otherwise show alphabet-containing values in error
def check_no_alphabet_only_numeric(unexpected, column_name, table_name):
    found_alphabet = [n for n in unexpected if re.search(r'[a-zA-Z]', n)]
    if len(found_alphabet) > 0:
        LOG.error(f'{column_name} in {table_name} has the following alphabet-containing value(s): {", ".join(found_alphabet)}.')
        found_error()
    LOG.warning(f'{column_name} in {table_name} has the following non-standard value(s): {", ".join([n for n in unexpected if n not in found_alphabet])}. Please check if they are correct.')


# Check column values contain integers in specific range or UNKNOWN or _ only
def check_range_with_unknowns(df, column_name, table_name, dp_path, lo, hi):
    check_case(df, column_name, table_name, dp_path)
    uniques_non_empty = get_uniques_non_empty(df, column_name)
    unexpected = [unique for unique in uniques_non_empty if (not unique.isdecimal() or not lo <= int(unique) <= hi) and unique != 'UNKNOWN']
    
    if len(unexpected) == 0:
        return
    
    LOG.error(f'{column_name} in {table_name} has the following unexpected value(s): {", ".join(unexpected)}.')
    found_error()


# Modify the database, correct all strings to uppercase in the selected column
def db_update_to_upper(table_name, column_name, dp_path):
    with sqlite3.connect(dp_path) as con:
        cur = con.cursor()
        cur.execute(f'''
                    UPDATE {table_name}
                    SET {column_name} = UPPER({column_name})
                    ''')
        con.commit()


def found_error():
    global FOUND_ERRORS
    FOUND_ERRORS = True


if __name__ == '__main__':
    main()