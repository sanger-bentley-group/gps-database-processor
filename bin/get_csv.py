# This module contains functions for generating various .csv. 


import pandas as pd
import csv
import bin.config as config


# Generate table4 based on data from table1
def get_table4(table1, table3, table4):
    config.LOG.info(f'Generating {table4} now...')

    # Read table1 and table3 for inferring data in table4
    df_meta, df_analysis = read_tables(table1, table3)

    # Create a partial table4 dataframe based on a subset of table1
    df_table4_meta = df_meta[['Public_name', 'Country', 'Region', 'City', 'Year', 'Age_years', 'Age_months', 'Age_days', 'Clinical_manifestation', 'Source']].copy()

    global UPDATED_COORDINATES
    UPDATED_COORDINATES = False
    df_table4_meta = df_table4_meta.apply(get_coordinate, axis=1)
    if UPDATED_COORDINATES:
        config.LOG.warning(f'Please verify the new coordinate(s). If any is incorrect, modify the coordinate in {config.COORDINATES_FILE} and re-run this tool.')

    df_table4_meta = df_table4_meta.apply(get_resolution, axis=1)
    df_table4_meta = df_table4_meta.apply(get_pcv_info, axis=1)
    df_table4_meta = df_table4_meta.apply(get_less_than_5_years_old, axis=1)

    # Get the Manifestation based on the values in 'Clinical_manifestation', 'Source' and the 'data/manifestations.csv' reference table.
    df_table4_meta['Manifestation'] = df_table4_meta.set_index(['Clinical_manifestation', 'Source']).index.map(config.MANIFESTATIONS.get)

    # Create a partial table4 dataframe based on a subset of table3
    df_table4_analysis = df_analysis[['Public_name', 'In_silico_serotype', 'Duplicate']].copy()
    df_table4_analysis.drop(df_table4_analysis[df_table4_analysis['Duplicate'] != 'UNIQUE'].index, inplace=True)
    df_table4_analysis = df_table4_analysis.apply(get_vaccines_covered, axis=1)

    # Merge the partial table4 dataframes
    df_table4 = df_table4_meta.merge(df_table4_analysis, on='Public_name', how='outer', validate='one_to_one')
    # Get the published status based on the values in 'Public_name' and the 'data/published_public_names.txt' reference list.
    df_table4['Published'] = 'N'
    df_table4['Published'].where(~df_table4['Public_name'].isin(config.PUBLISHED_PUBLIC_NAMES), other='Y', inplace=True)
    # Replace all NA values with '_'
    df_table4.fillna('_', inplace=True)

    # Drop all columns that are not in the schema of table4
    output_cols = ('Public_name', 'Latitude', 'Longitude', 'Resolution', 'Vaccine_period', 'Introduction_year', 'PCV_type', 'Manifestation', 'Less_than_5_years_old', 'PCV7', 'PCV10_GSK', 'PCV10_Pneumosil', 'PCV13', 'PCV15', 'PCV20', 'PCV21', 'PCV24', 'IVT25', 'Published')
    df_table4.drop(columns=[col for col in df_table4 if col not in output_cols], inplace=True)
    df_table4 = df_table4.reindex(columns = output_cols)
    
    # Export table4
    df_table4.to_csv(table4, index=False)
    config.LOG.info(f'{table4} is generated.')


# Generate Monocle table based on table1 - 4
def get_monocle(table1, table2, table3, table4, table_monocle):
    config.LOG.info(f'Generating Monocle table now...')

    df_meta, df_qc, df_analysis, df_table4 = read_tables(table1, table2, table3, table4)

    # Only preserve QC Passed, UNIQUE and Published for Monocle table
    df_qc.drop(df_qc[~df_qc['QC'].isin(['PASS', 'PASSPLUS'])].index, inplace=True)
    df_analysis.drop(df_analysis[df_analysis['Duplicate'] != 'UNIQUE'].index, inplace=True)
    df_table4.drop(df_table4[df_table4['Published'] != 'Y'].index, inplace=True)

    # Drop columns that do not exist in Monocle table
    df_meta.drop(columns=['aroE', 'ddl', 'gdh', 'gki', 'recP', 'spi', 'xpt'], inplace=True)
    df_qc.drop(columns=['Supplier_name'], inplace=True)
    df_analysis.drop(columns=['No_of_genome', 'Paper_1'], inplace=True)
    df_table4.drop(columns=['Published'], inplace=True)

    # Merge all 4 tables and only retain samples exist in all 4
    df = df_meta.merge(df_analysis, how='inner', on='Public_name', validate='one_to_one')
    df = df.merge(df_qc, how='inner', on='Lane_id', validate='one_to_one')
    df = df.merge(df_table4, how='inner', on='Public_name', validate='one_to_one')

    # Workaround for Monocle not supporting empty Submitting_institution
    df['Submitting_institution'].replace('_', 'UNKNOWN', inplace=True)

    # Export Monocle Table
    df.replace('_', '', inplace=True)
    df.to_csv(table_monocle, index=False)

    config.LOG.info(f'{table_monocle} is generated.')


# Read the tables into Pandas dataframes for processing
def read_tables(*arg):
    dfs = []
    for table in arg:
        dfs.append(pd.read_csv(table, dtype=str))
    return dfs


# Get coordinates based on 'Country', 'Region', 'City'.
# Use pre-existing data in 'data/coordinates.csv' if possible,
# otherwise search with geopy and add to 'data/coordinates.csv'
def get_coordinate(row):
    country, region, city = row['Country'], row['Region'], row['City']
    country_region_city = ','.join((country, region, city))
    
    if country_region_city == '_,_,_':
        latitude, longitude = '_', '_'
    elif country_region_city in config.COORDINATES:
        latitude, longitude = config.COORDINATES[country_region_city]
    else:
        # Initialise and use config.MAPBOX_GEOCODER to sesarch for coordinates
        config.get_geocoder() 
        coordinate = config.MAPBOX_GEOCODER.geocode(country_region_city)
        latitude, longitude = coordinate.latitude, coordinate.longitude

        # Save new coordinate to file and reload coordinates dictionary from file
        with open(config.COORDINATES_FILE, 'a') as f:
            writer = csv.writer(f)
            writer.writerow([country_region_city, latitude, longitude])
        config.read_coordinates()

        global UPDATED_COORDINATES
        UPDATED_COORDINATES = True
        config.LOG.warning(f'New location {country_region_city} is found, the coordinate is determined to be {latitude}, {longitude} and added to "{config.COORDINATES_FILE}".')
    
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


# Get vaccine period, introduction year, PCV type based on the values in 'Country', 'Year' and the 'data/pcv_introduction_year.csv' reference table.
def get_pcv_info(row):
    country, region, year = row['Country'], row['Region'], row['Year']

    # Workaround for non-country level entry that has separated PCV programmes
    if region in {'HONG KONG'}:
        country = region
    
    output_intro_year = '_'
    output_pcv = '_'

    # row without valid 'Year' will have '_' instead of 'PREPCV' or others for 'Vaccine_period'.
    # 'Year' of the row must be larger than the introduction year of that PCV to be considered within that vaccine period, same year is not considered. 
    try:
        year = int(year)
        output_vaccine_period = 'PREPCV'

        for intro_year, pcv in config.PCV_INTRO_YEARS[country]:
            intro_year = int(intro_year)
            if not year > intro_year:
                break
            output_vaccine_period = f'POST{pcv}-{year - intro_year}YR'
            output_intro_year = intro_year
            output_pcv = pcv
    except ValueError:
        output_vaccine_period = '_'

    row['Vaccine_period'] = output_vaccine_period
    row['Introduction_year'] = output_intro_year
    row['PCV_type'] = output_pcv
    return row


# Get whether the age of of the row is less than 5 years old or not.
def get_less_than_5_years_old(row):
    age_years, age_months, age_days = row['Age_years'], row['Age_months'], row['Age_days']

    if age_years == '_' and age_months == '_' and age_days == '_':
        less_than_5_years_old = '_'
    elif age_years == '_':
        less_than_5_years_old = 'Y'
    elif age_years in config.NON_STANDARD_AGES:
        less_than_5_years_old = config.NON_STANDARD_AGES[age_years]
    else:
        if float(age_years) < 5:
            less_than_5_years_old = 'Y'
        else:
            less_than_5_years_old = 'N'

    row['Less_than_5_years_old'] = less_than_5_years_old
    return row


# Get whether the in silico serotype of the row is targeted by each vaccine.
def get_vaccines_covered(row):
    serotype = row['In_silico_serotype']

    for pcv, serotypes in config.PCV_VALENCY.items():
        if serotype in serotypes:
            row[pcv] = 'Y'
        else:
            row[pcv] = 'N'

    return row