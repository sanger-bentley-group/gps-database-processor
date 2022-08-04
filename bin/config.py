# This module saves the global variables shared by all modules.


import bin.colorlog
import csv

def init():
    # Provide global logger to all functions
    global LOG
    LOG = bin.colorlog.get_log()

    # Path to coordinates file
    global COORDINATES_FILE
    COORDINATES_FILE = 'data/coordinates.csv'

    # Provide global dictionary for acessing pre-existing coordinates
    global COORDINATES
    with open(COORDINATES_FILE, 'r') as f:
        reader = csv.reader(f)
        next(reader)
        COORDINATES = {}
        for country_region_city, latitude, longitude in reader:
            COORDINATES[country_region_city] = (latitude, longitude)