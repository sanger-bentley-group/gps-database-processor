# This module saves the global variables shared by all modules.


import bin.colorlog as colorlog
import data.api_keys as api_keys
import csv
from collections import defaultdict


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

    # Path to manifestations file and store content as global dictionary MANIFESTATIONS
    global MANIFESTATIONS_FILE
    MANIFESTATIONS_FILE = 'data/manifestations.csv'
    read_manifestations()

    # Path to published public names file and store content as global set PUBLISHED_PUBLIC_NAMES
    published_public_names_file = 'data/published_public_names.txt'
    read_published_public_names(published_public_names_file)

    # Path to vaccines introduction year file and store content as global dictionary PCV_INTRO_YEARS
    global PCV_INTRO_YEARS_FILE
    PCV_INTRO_YEARS_FILE = 'data/pcv_introduction_year.csv'
    read_pcv_intro_years()

    # Path to vaccines valency file and store content as global dictionary PCV_VALENCY
    pcv_valency_file = 'data/pcv_valency.csv'
    read_pcv_valency(pcv_valency_file)

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


# Provide global dictionary for acessing the clinical manifestation and source combination results in which manifestation
def read_manifestations():
    global MANIFESTATIONS
    with open(MANIFESTATIONS_FILE, 'r') as f:
        reader = csv.reader(f)
        next(reader)
        MANIFESTATIONS = {}
        for Clinical_manifestation, Source, Manifestation in reader:
            MANIFESTATIONS[Clinical_manifestation, Source] = Manifestation
            

# Provide global set for acessing published public names
def read_published_public_names(published_public_names_file):
    global PUBLISHED_PUBLIC_NAMES
    PUBLISHED_PUBLIC_NAMES = set(line.strip() for line in open(published_public_names_file))


# Provide global dictionary for acessing vaccines introduction years in each country
def read_pcv_intro_years():
    global PCV_INTRO_YEARS
    with open(PCV_INTRO_YEARS_FILE, 'r') as f:
        reader = csv.reader(f)
        next(reader)
        PCV_INTRO_YEARS = defaultdict(list)
        for country, pcv, intro_year in reader:
            PCV_INTRO_YEARS[country].append((intro_year, pcv))
        
    for country in PCV_INTRO_YEARS:
        PCV_INTRO_YEARS[country].sort()


# Provide global dictionary for acessing valency of vaccines
def read_pcv_valency(pcv_valency_file):
    global PCV_VALENCY
    with open(pcv_valency_file, 'r') as f:
        reader = csv.reader(f)
        next(reader)
        PCV_VALENCY = {}
        for pcv, serotypes in reader:
            PCV_VALENCY[pcv] = set(serotypes.split(','))