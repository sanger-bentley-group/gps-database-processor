#!/usr/bin/env python

import argparse
import sys
import bin.config as config
import bin.validator as validator
import bin.get_csv as get_csv
import bin.get_json as get_json


def main():
    config.init()

    args = parse_args()
    table1, table2, table3, table4, version, check = args.table1, args.table2, args.table3, args.table4, args.version, args.check

    # Default names of Monocle Table and Data JSON
    table_monocle = 'table_monocle.csv'
    data = 'data.json'

    validator.validate(table1, table2, table3, version, check)

    if check:
        return

    get_csv.get_table4(table1, table3, table4)
    get_csv.get_monocle(table1, table2, table3, table4, table_monocle)
    get_json.get_data(table_monocle, data)

    config.LOG.info('The processing is completed. Database is validated and all files are generated.')


# Parse optional arguments for overriding default file names
def parse_args():
    parser = argparse.ArgumentParser(
        allow_abbrev=False,
        description='Process GPS (Global Pneumococcal Sequencing Project) database updates. This tool validates table1 (Metadata), table2 (QC) and table3 (Analysis) provided by the user; then generates table4, Monocle Table and Data JSON of the GPS Database Overview.',
        epilog='If you have updated any files (except api_keys.py) in the "data" directory, please submit a PR to https://github.com/sanger-bentley-group/gps-database-processor',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )

    parser.add_argument(
        '-m', '--meta',
        dest='table1',
        default='table1.csv',
        help='file name of table1 metadata'
    )

    parser.add_argument(
        '-q', '--qc',
        dest='table2',
        default='table2.csv',
        help='file name of table2 QC'
    )

    parser.add_argument(
        '-a', '--analysis',
        dest='table3',
        default='table3.csv',
        help='file name of table3 analysis'
    )

    parser.add_argument(
        '-o', '--output',
        dest='table4',
        default='table4.csv',
        help='file name of table4'
    )

    parser.add_argument(
        '-v', '--version',
        dest='version',
        default='1',
        metavar='N',
        help='version of the GPS database'
    )

    parser.add_argument(
        '-c', '--check',
        action="store_true",
        help='perform validation only'
    )

    args = parser.parse_args()

    # Check all input and output files are .csv
    non_csv = []

    for arg in vars(args):
        match arg:
            case "version":
                version = getattr(args, arg)
                if version not in ("1", "2"):
                    config.LOG.critical(f'This tool only work with GPS Database version 1 and 2. Please select a compatible version number.')
                    sys.exit(1)
            case str(x) if x.startswith("table"):
                filename = getattr(args, arg)
                if filename[-4:] != '.csv':
                    non_csv.append(filename)
    
    if non_csv:
        config.LOG.critical(f'The following file name(s) do not have ".csv" as their file extension: {", ".join(non_csv)}. This tool only work with ".csv" files. The process will now be halted.')
        sys.exit(1)

    return args


if __name__ == "__main__":
    main()