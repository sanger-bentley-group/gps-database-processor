# This module contains functions for generating various .csv. 


import pandas as pd
import bin.config as config
from geopy.geocoders import MapBox
import csv
import sys


# Generate table4.csv based on data from table1.csv
def get_table4(table1, table3):
    global LOG
    LOG = config.LOG

    # Read table1 and table3 for inferring data in table4
    df_meta, df_analysis = read_tables(table1, table3)

    # Create a partial table4 dataframe based on a subset of table1
    df_table4_meta = df_meta[['Public_name', 'Country', 'Region', 'City']].copy()

    global UPDATED_COORDINATES
    UPDATED_COORDINATES = False
    geocoder = MapBox(api_key=config.MAPBOX_API_KEY)
    df_table4_meta = df_table4_meta.apply(get_coordinate_and_res, geocoder=geocoder, axis=1)
    if UPDATED_COORDINATES:
        LOG.warning(f'Please verify the new coordinate(s). If any is incorrect, modify the coordinate in {config.COORDINATES_FILE} and re-run this tool.')

    df_table4_meta = df_table4_meta.apply(get_resolution, axis=1)

    # Create a partial table4 dataframe based on a subset of table3
    df_table4_analysis = df_analysis[['Public_name', 'In_silico_serotype', 'Duplicate']].copy()
    df_table4_analysis.drop(df_table4_analysis[df_table4_analysis['Duplicate'] != 'UNIQUE'].index, inplace = True)
    df_table4_analysis = df_table4_analysis.apply(get_vaccines, axis=1)

    # Merge the partial table4 dataframes
    df_table4 = pd.merge(df_table4_meta, df_table4_analysis, on='Public_name', how='left', validate='one_to_one')
    # Replace all NA values with '_'
    df_table4.fillna('_', inplace=True)

    # Drop all columns that are not in the schema of table4
    output_cols = ('Public_name', 'Latitude', 'Longitude', 'Resolution', 'Vaccine_period', 'Introduction_year', 'PCV_type', 'Manifest_type', 'Less_than_5_years_old', 'PCV7', 'PCV10_GSK', 'PCV10_Pneumosil', 'PCV13', 'PCV15', 'PCV20', 'PCV21', 'PCV24', 'IVT-25', 'Published')
    df_table4.drop(columns=[col for col in df_table4 if col not in output_cols], inplace=True)
    
    # Export table4
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
def get_coordinate_and_res(row, geocoder):
    country, region, city = row['Country'], row['Region'], row['City']
    country_region_city = ','.join((country, region, city))
    
    if country_region_city in config.COORDINATES:
        latitude, longitude = config.COORDINATES[country_region_city]
    else:
        try:
            coordinate = geocoder.geocode(country_region_city)
        except:
            LOG.critical(f'{country_region_city} has no known coordinate, but no valid Mapbox API key is provided. Please provide Mapbox API key in "data/api_keys.py" or manually enter coordinate of {country_region_city} in "data/coordinates.csv", then re-run this tool. The process will now be halted.')
            sys.exit(1)
        
        latitude, longitude = coordinate.latitude, coordinate.longitude

        # Save new coordinate to file and reload coordinates dictionary from file
        with open(config.COORDINATES_FILE, 'a') as f:
            writer = csv.writer(f)
            writer.writerow([country_region_city, latitude, longitude])
        config.read_coordinates()

        global UPDATED_COORDINATES
        UPDATED_COORDINATES = True
        LOG.warning(f'New location {country_region_city} is found, the coordinate is determined to be {latitude}, {longitude} and added to "{config.COORDINATES_FILE}".')
    
    row['Latitude'] = latitude
    row['Longitude'] = longitude
    return row


# Get resolution based on the right-most non-empty values in 'Country', 'Region', 'City'.
def get_resolution(row):
    country, region, city = row['Country'], row['Region'], row['City']

    resolution = '_'
    if country not in ('_', ''):
        resolution = '0'
    if region not in ('_', ''):
        resolution = '1'
    if city not in ('_', ''):
        resolution = '2'
    row['Resolution'] = resolution

    return row


# Get whether the in silico serotype of the row is included in each vaccine.
def get_vaccines(row):
    serotype = row['In_silico_serotype']

    for vaccine, serotypes in config.VACCINES.items():
        if serotype in serotypes:
            row[vaccine] = 'Y'
        else:
            row[vaccine] = 'N'

    return row