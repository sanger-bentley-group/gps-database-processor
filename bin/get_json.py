# This module contains 'get_data' function and its supporting functions.
# 'get_data' function takes the generated Monocle table1 as input and generate data.json for the GPS Database Overview


import pandas as pd
import json
import bin.config as config


# Set max age and bin size for grouping ages in country part of data.json
MAX_AGE = 120
BIN_SIZE = 10


# Generate data.json based on Monocle table1
def get_data(table1):
    config.LOG.info(f'Generating data.json now...')

    # Scaffold of the data.json
    output = {
        'summary': {
            'country': {},
            'vaccine_period': {},
            'manifestation': {},
            'year_of_collection': {},
            'age': {}
        },
        'country': {},
    }

    # Read generated Monocle table1
    table1_monocle = f'{table1[:-4]}_monocle.csv'
    df = pd.read_csv(table1_monocle, dtype=str)

    # Workaround for non-country level entry that has separated PCV programmes
    for region in {'HONG KONG'}:
        df.loc[df['Region'] == region, 'Country'] = region

    # Prepare columns for groupby functions
    df['Vaccine_period'] = df['Vaccine_period'].str.split('-').str[0]
    df = df.apply(simplify_age, axis=1)

    # Generate summary part of data.json
    # Sort country, vaccine period, manifestation in descending order by values
    # Sort year of collection, age in ascending order by index with NaN at the first position
    output_summary_country = df.groupby('Country', dropna=False).size().sort_values(ascending=False).to_dict()
    output['summary']['country'] = {config.COUNTRY_ALPHA2.get(country, country): val for country, val in output_summary_country.items()}
    output['summary']['vaccine_period'] = df.groupby('Vaccine_period', dropna=False).size().sort_values(ascending=False).to_dict()
    output['summary']['manifestation'] = df.groupby('Manifestation', dropna=False).size().sort_values(ascending=False).to_dict()
    output['summary']['year_of_collection'] = df.groupby('Year', dropna=False).size().sort_index(key=lambda x: x.astype('Int64'), na_position='first').to_dict()
    output['summary']['age'] = df.groupby('Simplified_age', dropna=False).size().sort_index(key=lambda x: x.astype('Int64'), na_position='first').to_dict()

    # Go through country by country to generate per-country part of data.json
    countries = sorted(df['Country'].dropna().unique().tolist())
    for country in countries:
        # Get and prepare df_country for the current country; build scaffold for output of the current country
        
        # Skip non-country entity (e.g. West Africa)
        try:
            alpha2 = config.COUNTRY_ALPHA2[country]
        except KeyError:
            continue

        df_country = df[df['Country'] == country].copy()
        df_country['Simplified_age'] = df_country['Simplified_age'].astype('Int64')
        output['country'][alpha2] = {'total': len(df_country.index), 'age': {}, 'manifestation': {}, 'vaccine_period': {}}
        
        # Get all years within sample year range of that country
        # years_min and years_max would be None if there is 0 non-NaN year
        year_range, years_min, years_max = get_year_range(df_country['Year'])

        # Get vaccine periods in country part of data.json if there is at least one non-NaN year
        if not None in (years_min, years_max):
            output['country'][alpha2]['vaccine_period'] = get_vaccine_periods(years_min, years_max, country)

        # Go through year by year
        for year in year_range:
            # Get df_country_year depending on year is NaN or numeric value
            if pd.isna(year):
                df_country_year = df_country[pd.isna(df_country['Year'])]
                # Change to "NaN" string to allow hashing as dictionary key
                year = 'NaN'
            else:
                df_country_year = df_country[df_country['Year'] == year]
            
            # Get age group sizes per year in country part of data.json
            output['country'][alpha2]['age'][year] = get_age_size(df_country_year)

            # Generate manifestation sizes per year in country part of data.json
            output['country'][alpha2]['manifestation'][year] = df_country_year.groupby('Manifestation', dropna=False).size().sort_values(ascending=False).to_dict()

    # Save data.json to file
    with open('data.json', 'w') as f:
        json.dump(output, f, indent=4)
    
    config.LOG.info('data.json is generated.')


# Simplify age to a integer value based on year, or NaN if the precise age year cannot be determined
def simplify_age(row):
    age_years, age_months, age_days = row['Age_years'], row['Age_months'], row['Age_days']

    if (pd.isna(age_years) and pd.isna(age_months) and pd.isna(age_days)) or age_years in config.NON_STANDARD_AGES:
        simplified_age = pd.NA
    elif pd.isna(age_years) or float(age_years) < 1:
        simplified_age = '0'
    else:
        simplified_age = str(int(age_years))
    
    row['Simplified_age'] = simplified_age
    return row


# Generate all years, min and max of years (if having at least one non-NaN) within sample year range of that country; put NaN in the beginning of the list if it exists
def get_year_range(years_series):
    year_range = []
    if years_series.isna().values.any():
        year_range.append(pd.NA)
    
    years_min = years_max = None
    years_unique = years_series.dropna().unique().astype(int).tolist()
    if years_unique:
        years_min, years_max = min(years_unique), max(years_unique)
        year_range.extend([str(i) for i in range(years_min, years_max + 1)])
    
    return year_range, years_min, years_max


# Generate vaccine periods in country part of data.json
def get_vaccine_periods(years_min, years_max, country):
    vaccine_periods = [[years_min, years_max, 'Pre-PCV']]
    for year, pcv in config.PCV_INTRO_YEARS[country]:
        year = int(year)
        pre_years_min, pre_years_max = vaccine_periods[-1][0], vaccine_periods[-1][1]
        if year < pre_years_min:
            vaccine_periods[-1][2] = f'Post-{pcv}'
        elif year < vaccine_periods[-1][1]:
            vaccine_periods[-1][1] = year
            vaccine_periods.append([year + 1, pre_years_max, f'Post-{pcv}'])
    
    return {f'{start},{end}': period for start, end, period in vaccine_periods}


# Generate binned age group size, remove age group with 0 samples, add NaN age group if having samples with unknown age
def get_age_size(df):
    bins = [i for i in range(0, MAX_AGE + 1, BIN_SIZE)]
    labels = [f'{i} - {i + (BIN_SIZE - 1)}' for i in range(0, MAX_AGE, BIN_SIZE)]
    age_size = df.groupby(pd.cut(df['Simplified_age'], bins=bins, labels=labels, include_lowest=True, right=False)).size().to_dict()
    
    for key, value in list(age_size.items()):
        if value == 0:
            del age_size[key]
    
    unknown_age_size = len(df[pd.isna(df['Simplified_age'])])
    if unknown_age_size:
        age_size["NaN"] = unknown_age_size
    
    return age_size