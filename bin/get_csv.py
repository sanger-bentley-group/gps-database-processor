# This module contains functions for generating various .csv. 


import pandas as pd


# Generate table4.csv based on data from table1.csv and table3.csv
def get_table4(table1, table3):
    df_meta, df_analysis = read_tables(table1, table3)


# Read the tables into Pandas dataframes for processing
def read_tables(table1, table3):
    dfs = []
    for table in table1, table3:
        dfs.append(pd.read_csv(table, dtype=str))
    return dfs