# This module contains 'get_data' function and its supporting functions.
# 'get_data' function takes the generated Monocle table1 as input and generate data.json for GPS Database Overview


import pandas as pd
import json
import bin.config as config


# Generate data.json based on Monocle table1
def get_data(table1):
    config.LOG.info(f'Generating data.json now...')

    # Template of the data.json
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

    # Prepare columns for groupby functions
    df['Vaccine_period'] = df['Vaccine_period'].str.split('-').str[0]
    df = df.apply(simplify_age, axis=1)

    # Generate summary part of data.json
    # Sort country, vaccine period, manifestation in descending order by values; sort year of collection, age in ascending order by index with NA at the first position
    output['summary']['country'] = df.groupby('Country', dropna=False).size().sort_values(ascending=False).to_dict()
    output['summary']['vaccine_period'] = df.groupby('Vaccine_period', dropna=False).size().sort_values(ascending=False).to_dict()
    output['summary']['manifestation'] = df.groupby('Manifestation', dropna=False).size().sort_values(ascending=False).to_dict()
    output['summary']['year_of_collection'] = df.groupby('Year', dropna=False).size().sort_index(key=lambda x: x.astype('Int64'), na_position='first').to_dict()
    output['summary']['age'] = df.groupby('Simplified_age', dropna=False).size().sort_index(key=lambda x: x.astype('Int64'), na_position='first').to_dict()

    # Generate per-country part of data.json
    countries = sorted(df['Country'].dropna().unique().tolist())
    for country in countries:
        alpha2 = config.COUNTRY_ALPHA2[country]
        output['country'][alpha2] = {'age': {}, 'manifestation': {}, 'vaccine_period': {}}
        


    # Save data.json
    with open('data.json', 'w') as f:
        json.dump(output, f, indent=4)
    
    config.LOG.info('data.json is generated.')


# Simplify age to year only in a integer value
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