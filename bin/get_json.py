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

    print(table1)

    with open('data.json', 'w') as f:
        json.dump(output, f, indent=4)
    
    config.LOG.info('data.json is generated.')