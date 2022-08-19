from multiprocessing.dummy.connection import families
import pandas as pd
import json
import bin.config as config


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

    table1_monocle = f'{table1[:-4]}_monocle.csv'
    df = pd.read_csv(table1_monocle, dtype=str)

    output['summary']['country'] = df.groupby('Country', dropna=False).size().to_dict()

    df['Vaccine_period'] = df['Vaccine_period'].str.split('-').str[0]
    output['summary']['vaccine_period'] = df.groupby('Vaccine_period', dropna=False).size().to_dict()

    output['summary']['manifestation'] = df.groupby('Manifestation', dropna=False).size().to_dict()

    output['summary']['year_of_collection'] = df.groupby('Year', dropna=False).size().to_dict()

    with open('data.json', 'w') as f:
        json.dump(output, f, indent=4)
    
    config.LOG.info('data.json is generated.')