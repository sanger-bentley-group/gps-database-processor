import argparse
import bin.config as config
import bin.validator as validator
import bin.get_csv as get_csv


def main():
    args = parse_args()
    table1, table2, table3, table4 = args.table1, args.table2, args.table3, args.table4

    config.init()

    validator.validate(table1, table2, table3)
    get_csv.get_table4(table1, table3, table4)
    get_csv.get_monocle(table1, table2, table3, table4)

    config.LOG.info('The processing is completed. Database is validated and all files are generated.')


def parse_args():
    parser = argparse.ArgumentParser(
        allow_abbrev=False,
        description='Process GPS (Global Pneumococcal Sequencing Project) database updates. This tool validates table1 (metadata), table2 (qc) and table3 (analysis) provided by the user; then generates table4, Monocle-ready tables and data.json of the GPS Database Overview.',
        epilog='If you have updated any files (except api_keys.py) in the "data" directory, please submit a PR to github.com/HarryHung/gps-database-processor'
    )

    parser.add_argument(
        '-m', '--meta',
        dest='table1',
        metavar='your_file_name.csv',
        default='table1.csv',
        help='(Input file) Override the default table1 (metadata) file name of table1.csv'
    )

    parser.add_argument(
        '-q', '--qc',
        dest='table2',
        metavar='your_file_name.csv',
        default='table2.csv',
        help='(Input file) Override the default table2 (qc) file name of table2.csv'
    )

    parser.add_argument(
        '-a', '--analysis',
        dest='table3',
        metavar = 'your_file_name.csv',
        default='table3.csv',
        help='(Input file) Override the default table3 (analysis) file name of table3.csv'
    )

    parser.add_argument(
        '-o', '--output',
        dest='table4',
        metavar = 'your_file_name.csv',
        default='table4.csv',
        help='(Output file) Override the default table4 file name of table4.csv'
    )

    return parser.parse_args()


if __name__ == "__main__":
    main()