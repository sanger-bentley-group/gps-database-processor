# This module saves the global variables shared by all modules.


import bin.colorlog as colorlog
import data.api_keys as api_keys
import csv


def init():
    # Provide global logger to all functions
    global LOG
    LOG = colorlog.get_log()

    # Path to coordinates file and store content as global dictionary
    global COORDINATES_FILE
    COORDINATES_FILE = 'data/coordinates.csv'
    read_coordinates()

    # Path to vaccines valency file and store content as global dictionary
    vaccines_valency_file = 'data/vaccines_valency.csv'
    read_vaccines_valency(vaccines_valency_file)

    # Path to non-standard ages file and store content as global dictionary
    global NON_STANDARD_AGES_FILE
    NON_STANDARD_AGES_FILE = 'data/non_standard_ages.csv'
    read_non_standard_ages()

    global MAPBOX_API_KEY
    MAPBOX_API_KEY = api_keys.mapbox


# Provide global dictionary for acessing pre-existing coordinates
def read_coordinates():
    global COORDINATES
    with open(COORDINATES_FILE, 'r') as f:
        reader = csv.reader(f)
        next(reader)
        COORDINATES = {}
        for country_region_city, latitude, longitude in reader:
            COORDINATES[country_region_city] = (latitude, longitude)


# Provide global dictionary for acessing valency of vaccines
def read_vaccines_valency(vaccines_valency_file):
    global VACCINES_VALENCY
    with open(vaccines_valency_file, 'r') as f:
        reader = csv.reader(f)
        next(reader)
        VACCINES_VALENCY = {}
        for vaccine, serotypes in reader:
            VACCINES_VALENCY[vaccine] = set(serotypes.split(','))


# Provide global dictionary for acessing whether non_standard_ages are less than 5 years old or not
def read_non_standard_ages():
    global NON_STANDARD_AGES
    with open(NON_STANDARD_AGES_FILE, 'r') as f:
        reader = csv.reader(f)
        next(reader)
        NON_STANDARD_AGES = {}
        for age, less_than_5_years_old in reader:
            NON_STANDARD_AGES[age] = less_than_5_years_old