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

    # Workaround for non-country level entry that has separated PCV programmes
    for region in {'HONG KONG'}:
        df.loc[df['Region'] == region, 'Country'] = region

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

    # Set max age and bin size for grouping ages
    max_age = 120
    bin_size = 10

    # Go through country by country
    for country in countries:
        alpha2 = config.COUNTRY_ALPHA2[country]
        df_country = df[df['Country'] == country].copy()
        df_country['Simplified_age'] = df_country['Simplified_age'].astype('Int64')
        output['country'][alpha2] = {'age': {}, 'manifestation': {}, 'vaccine_period': {}}
        
        # Get all years within sample year range of that country; put NaN in the beginning of the list if exists
        year_range = []
        years_series = df_country['Year']
        if years_series.isna().values.any():
            year_range.append(pd.NA)
        years_unique = years_series.dropna().unique().astype(int).tolist()
        if years_unique:
            years_min, years_max = min(years_unique), max(years_unique)
            year_range.extend([str(i) for i in range(years_min, years_max + 1)])

            # Generate vaccine periods in country part of data.json
            vaccine_periods = [[years_min, years_max, 'Pre-PCV']]
            for year, pcv in config.PCV_INTRO_YEARS[country]:
                year = int(year)
                pre_years_min, pre_years_max = vaccine_periods[-1][0], vaccine_periods[-1][1]
                if year < pre_years_min:
                    vaccine_periods[-1][2] = f'Post-{pcv}'
                elif year < vaccine_periods[-1][1]:
                    vaccine_periods[-1][1] = year
                    vaccine_periods.append([year + 1, pre_years_max,f'Post-{pcv}'])
            
            vaccine_periods = {f'{start},{end}': period for start, end, period in vaccine_periods}
            output['country'][alpha2]['vaccine_period'] = vaccine_periods

        # Go through year by year
        for year in year_range:
            # Get df depending on year is NaN or numeric value
            if pd.isna(year):
                df_country_year = df_country[pd.isna(df_country['Year'])]
            else:
                df_country_year = df_country[df_country['Year'] == year]

            # Get binned age group size, remove age group with 0 samples, add NaN age group if having samples with unknown age
            age_size = df_country_year.groupby(pd.cut(df_country['Simplified_age'], bins=[i for i in range(0, max_age + 1, bin_size)], labels=[f'{i} - {i + (bin_size - 1)}' for i in range(0, max_age, bin_size)], include_lowest=True, right=False)).size().to_dict()
            for key, value in list(age_size.items()):
                if value == 0:
                    del age_size[key]
            unknown_age = len(df_country_year[pd.isna(df_country_year['Simplified_age'])])
            if unknown_age:
                age_size["NaN"] = unknown_age
            
            # Get manifestation group size
            manifestation_size = df_country_year.groupby('Manifestation', dropna=False).size().sort_values(ascending=False).to_dict()
            
            # If year is NaN, change to "NaN" string to allow hashing as dictionary key
            if pd.isna(year):
                 year = 'NaN'

            # Generate age group sizes and manifestation sizes per year in country part of data.json
            output['country'][alpha2]['age'][year] = age_size
            output['country'][alpha2]['manifestation'][year] = manifestation_size


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