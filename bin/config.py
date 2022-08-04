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