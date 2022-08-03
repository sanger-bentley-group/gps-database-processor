# This module contains functions for generating various .csv. 


import pandas as pd
import bin.config as config


# Generate table4.csv based on data from table1.csv and table3.csv
def get_table4(table1, table3):
    global LOG
    LOG = config.LOG

    df_meta, df_analysis = read_tables(table1, table3)

    df_table4 = df_meta[['Public_name', 'Country', 'City', 'Region']].copy()


    df_table4.to_csv('table4.csv', index=False)
    LOG.info('table4.csv is generated.')


# Read the tables into Pandas dataframes for processing
def read_tables(table1, table3):
    dfs = []
    for table in table1, table3:
        dfs.append(pd.read_csv(table, dtype=str))
    return dfs