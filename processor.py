import bin.config as config
import bin.validator as validator
import bin.get_csv as get_csv


def main(table1='table1.csv', table2='table2.csv', table3='table3.csv', table4='table4.csv'):
    config.init()

    validator.validate(table1, table2, table3)
    get_csv.get_table4(table1, table3, table4)
    get_csv.get_monocle_tables(table1, table2, table3, table4)

    config.LOG.info('The processing is completed. Database is validated and all files are generated.')


if __name__ == "__main__":
    main()