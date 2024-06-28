#!/usr/bin/env python

import argparse
import sys
import os
import bin.config as config
import bin.validator as validator
import bin.get_csv as get_csv
import bin.get_json as get_json


def main():
    config.init()

    args = parse_arguments()

    gps_provided = [(version + 1, path) for version, path in enumerate((args.gps1, args.gps2)) if bool(path)]

    check_arguments(args, gps_provided)

    # Validate all tables
    # Perform in-place letter case fix if not in validation only mode
    for (version, path) in gps_provided:
        validator.validate(path, version, args.check)

    # Early exit if in validation only mode
    if args.check:
        return

    # Generate table 4
    for (version, path) in gps_provided:
        get_csv.get_table4(path, args.location)

    # Generate Monocle data and GPS Database Overview data payload
    # WIP
    if args.monocle:
        monocle_table = get_csv.get_monocle(args.gps1, args.gps2)
        # get_json.get_data(monocle_table)

    config.LOG.info('The processing is completed. Data is validated and all files are generated.')


# Parse arguments
def parse_arguments():
    parser = argparse.ArgumentParser(
        allow_abbrev=False,
        description='Process GPS (Global Pneumococcal Sequencing Project) database updates. This tool validates data from GPS1 and/or GPS2; generates table4; generate Monocle Table and data payload of the GPS Database Overview.',
        epilog='If you have updated any files in the "data" directory, please submit a PR to https://github.com/sanger-bentley-group/gps-database-processor',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )

    parser.add_argument(
        '-1', '--gps1',
        default=None,
        help='path to directory of GPS1 data'
    )

    parser.add_argument(
        '-2', '--gps2',
        default=None,
        help='path to directory of GPS2 data'
    )

    parser.add_argument(
        '-c', '--check',
        action="store_true",
        help='perform validation only'
    )

    parser.add_argument(
        '-m', '--monocle',
        action="store_true",
        help='generate Monocle table and GPS Database Overview data payload from both GPS1 and GPS2'
    )

    parser.add_argument(
        '-l', '--location',
        action="store_true",
        help='get coordinates for locations not yet exist in data/coordinates.csv via MapBox API'
    )

    return parser.parse_args()

# Check input arguments are logical, and all tables exist in the path(s)
def check_arguments(args, gps_provided):
    if args.monocle:
        if len(gps_provided) != 2:
            config.LOG.critical(f'To generate Monocle-related data, paths to both GPS1 data and GPS2 data are required. The process will now be halted.')
            sys.exit(1)
    else:
        if len(gps_provided) == 0:
            config.LOG.critical(f'At least one path to either GPS1 data and GPS2 data is required. The process will now be halted.')
            sys.exit(1)
    
    for (ver, gps) in gps_provided:
        table1_path, table2_path, table3_path = (os.path.join(gps, table) for table in ("table1.csv", "table2.csv", "table3.csv"))
        if not all((os.path.isfile(table1_path), os.path.isfile(table2_path), os.path.isfile(table3_path))):
            config.LOG.critical(f'{gps} does not contain all required files (table1.csv, table2.csv, table3.csv). The process will now be halted.')
            sys.exit(1)


if __name__ == "__main__":
    main()