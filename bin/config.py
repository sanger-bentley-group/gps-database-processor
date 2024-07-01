# This module saves the global variables shared by all modules.



import csv
import configparser
import sys
import os
from collections import defaultdict
import geopy
import bin.colorlog as colorlog


def init():
    # Provide global logger to all functions
    global LOG
    LOG = colorlog.get_log()

    # Get processor.py path
    base_path = os.path.abspath(os.path.dirname(sys.argv[0]))

    # Path to coordinates file and store content as global dictionary COORDINATES
    global COORDINATES_FILE
    COORDINATES_FILE = f'{base_path}/data/coordinates.csv'
    read_coordinates()

    # Path to non-standard ages file and store content as global dictionary NON_STANDARD_AGES
    global NON_STANDARD_AGES_FILE
    NON_STANDARD_AGES_FILE = f'{base_path}/data/non_standard_ages.csv'
    read_non_standard_ages()

    # Path to manifestations file and store content as global dictionary MANIFESTATIONS
    global MANIFESTATIONS_FILE
    MANIFESTATIONS_FILE = f'{base_path}/data/manifestations.csv'
    read_manifestations()

    # Path to published public names file and store content as global set PUBLISHED_PUBLIC_NAMES
    global PUBLISHED_PUBLIC_NAMES_FILE
    PUBLISHED_PUBLIC_NAMES_FILE = f'{base_path}/data/published_public_names.txt'
    read_published_public_names()

    # Path to vaccines introduction year file and store content as global dictionary PCV_INTRO_YEARS
    global PCV_INTRO_YEARS_FILE
    PCV_INTRO_YEARS_FILE = f'{base_path}/data/pcv_introduction_year.csv'
    read_pcv_intro_years()

    # Path to vaccines valency file and store content as global dictionary PCV_VALENCY
    pcv_valency_file = f'{base_path}/data/pcv_valency.csv'
    read_pcv_valency(pcv_valency_file)

    # Path to ISO 3166-1 alpha-2 code of countries file and store content as global dictionary COUNTRY_ALPHA2 and ALPHA2_COUNTRY
    global ALPHA2_COUNTY_FILE
    ALPHA2_COUNTY_FILE = f'{base_path}/data/alpha2_country.csv'
    read_country_alpha2()


    # Path to locally saved configuration file with api keys
    global API_KEYS_FILE
    API_KEYS_FILE = f'{base_path}/config/api_keys.conf'
    


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
def read_published_public_names():
    global PUBLISHED_PUBLIC_NAMES
    PUBLISHED_PUBLIC_NAMES = set(line.strip() for line in open(PUBLISHED_PUBLIC_NAMES_FILE))


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


# Provide global dictionaries for acessing ISO 3166-1 alpha-2 code of countries, getting country name from code, and getting continent from country
def read_country_alpha2():
    global COUNTRY_ALPHA2
    global ALPHA2_COUNTRY
    global COUNTRY_CONTINENT
    with open(ALPHA2_COUNTY_FILE, 'r') as f:
        reader = csv.reader(f)
        next(reader)
        COUNTRY_ALPHA2 = {}
        ALPHA2_COUNTRY = {}
        COUNTRY_CONTINENT = {}
        for alpha2, country, continent in reader:
            COUNTRY_ALPHA2[country.upper()] = alpha2.upper()
            ALPHA2_COUNTRY[alpha2.upper()] = country
            COUNTRY_CONTINENT[country.upper()] = continent

# Initialise config.MAPBOX_GEOCODER if the variable does not exist yet 
def get_geocoder():
    if 'MAPBOX_GEOCODER' in globals():
        return

    # Read Mapbox API key from config/api_keys.conf, ask for the key if the .conf file does not exist yet
    global MAPBOX_GEOCODER
    api_keys = configparser.ConfigParser()
    api_keys.read(API_KEYS_FILE)
    if 'mapbox' not in api_keys:
        LOG.warning('Please provide Mapbox API key below for Latitude and Longitude auto-assignment (it will be saved locally for future use).')
        mapbox_api_key = input("Enter your Mapbox API key here: ")
        api_keys['mapbox'] = {}
    else:
        mapbox_api_key = api_keys['mapbox'].get('api_key')
    
    # Mapbox API key validity check
    while True:
        try:
            MAPBOX_GEOCODER = geopy.geocoders.MapBox(mapbox_api_key)
            MAPBOX_GEOCODER.geocode('United Kingdom,Cambridgeshire,Cambridge')
        except geopy.exc.GeocoderAuthenticationFailure:
            LOG.warning('The provided Mapbox API key is not valid, please enter a valid Mapbox API Key.')
            mapbox_api_key = input("Enter your Mapbox API key here: ")
            continue
        else:
            break

    # Update Mapbox API key in config/api_keys.conf if changed; create the .conf file if it does not exist yet
    if api_keys['mapbox'].get('api_key') != mapbox_api_key:
        api_keys['mapbox']['api_key'] = mapbox_api_key
        os.makedirs(os.path.dirname(API_KEYS_FILE), exist_ok=True)
        with open(API_KEYS_FILE, 'w') as f:
            api_keys.write(f)
