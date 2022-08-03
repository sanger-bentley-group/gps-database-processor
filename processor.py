from bin.validator import validator
from bin.get_csv import get_table4


def main(table1='table1.csv', table2='table2.csv', table3='table3.csv'):
    validator(table1, table2, table3)
    get_table4(table1, table3)


if __name__ == "__main__":
    main()