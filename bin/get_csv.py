# This module contains functions for generating various .csv. 


import pandas as pd
import bin.config as config


# Generate table4.csv based on data from table1.csv
def get_table4(table1, table3):
    global LOG
    LOG = config.LOG

    df_meta, df_analysis = read_tables(table1, table3)

    df_table4_meta = df_meta[['Public_name', 'Country', 'Region', 'City']].copy()
    df_table4_meta = df_table4_meta.apply(get_coordinate_and_res, axis=1)

    df_table4_analysis = df_analysis[['Public_name', 'In_silico_serotype', 'Duplicate']].copy()
    df_table4_analysis.drop(df_table4_analysis[df_table4_analysis['Duplicate'] != 'UNIQUE'].index, inplace = True)

    df_table4 = pd.merge(df_table4_meta, df_table4_analysis, on='Public_name', how='left', validate='one_to_one')
    df_table4.fillna('_', inplace=True)

    output_cols = ('Public_name', 'Latitude', 'Longitude', 'Resolution', 'Vaccine_period', 'Introduction_year', 'PCV_type', 'Manifest_type', 'Less_than_5_years_old', 'PCV7', 'PCV10_GSK', 'PCV10_Pneumosil', 'PCV13', 'PCV15', 'PCV20', 'PCV21', 'PCV24', 'IVT-25', 'Published')
    df_table4.drop(columns=[col for col in df_table4 if col not in output_cols], inplace=True)
    
    df_table4.to_csv('table4.csv', index=False)
    LOG.info('table4.csv is generated.')


# Read the tables into Pandas dataframes for processing
def read_tables(*arg):
    dfs = []
    for table in arg:
        dfs.append(pd.read_csv(table, dtype=str))
    return dfs


# Get coordinates based on 'Country', 'Region', 'City'.
# Use pre-existing data in 'data/coordinates.csv' if possible,
# otherwise search with geopy and add to 'data/coordinates.csv'
def get_coordinate_and_res(row):
    country, region, city = row['Country'], row['Region'], row['City']
    country_region_city = ','.join((country, region, city))
    if country_region_city in config.COORDINATES:
        latitude, longitude = config.COORDINATES[country_region_city]
        row['Latitude'] = latitude
        row['Longitude'] = longitude
    else:
        raise('NOT FOUND')
    return row