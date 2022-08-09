# This module saves the global variables shared by all modules.


import bin.colorlog as colorlog
import data.api_keys as api_keys
import csv


def init():
    # Provide global logger to all functions
    global LOG
    LOG = colorlog.get_log()

    # Path to coordinates file and store content as global dictionary COORDINATES
    global COORDINATES_FILE
    COORDINATES_FILE = 'data/coordinates.csv'
    read_coordinates()

    # Path to non-standard ages file and store content as global dictionary NON_STANDARD_AGES
    global NON_STANDARD_AGES_FILE
    NON_STANDARD_AGES_FILE = 'data/non_standard_ages.csv'
    read_non_standard_ages()

    # Path to manifest types file and store content as global dictionary MANIFEST_TYPES
    global MANIFEST_TYPES_FILE
    MANIFEST_TYPES_FILE = 'data/manifest_types.csv'
    read_manifest_types()

    # Path to published public names file and store content as global set PUBLISHED_PUBLIC_NAMES
    published_public_names_file = 'data/published_public_names.txt'
    read_published_public_names(published_public_names_file)

    # Path to vaccines valency file and store content as global dictionary VACCINES_VALENCY
    vaccines_valency_file = 'data/vaccines_valency.csv'
    read_vaccines_valency(vaccines_valency_file)

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


# Provide global dictionary for acessing whether non_standard_ages are less than 5 years old or not
def read_non_standard_ages():
    global NON_STANDARD_AGES
    with open(NON_STANDARD_AGES_FILE, 'r') as f:
        reader = csv.reader(f)
        next(reader)
        NON_STANDARD_AGES = {}
        for age, less_than_5_years_old in reader:
            NON_STANDARD_AGES[age] = less_than_5_years_old


# Provide global dictionary for acessing the clinical manifestation and source combination results in which manifest type
def read_manifest_types():
    global MANIFEST_TYPES
    with open(MANIFEST_TYPES_FILE, 'r') as f:
        reader = csv.reader(f)
        next(reader)
        MANIFEST_TYPES = {}
        for Clinical_manifestation, Source, Manifest_type in reader:
            MANIFEST_TYPES[Clinical_manifestation, Source] = Manifest_type
            

# Provide global set for acessing published public names
def read_published_public_names(published_public_names_file):
    global PUBLISHED_PUBLIC_NAMES
    PUBLISHED_PUBLIC_NAMES = set(line.strip() for line in open(published_public_names_file))


# Provide global dictionary for acessing valency of vaccines
def read_vaccines_valency(vaccines_valency_file):
    global VACCINES_VALENCY
    with open(vaccines_valency_file, 'r') as f:
        reader = csv.reader(f)
        next(reader)
        VACCINES_VALENCY = {}
        for vaccine, serotypes in reader:
            VACCINES_VALENCY[vaccine] = set(serotypes.split(','))