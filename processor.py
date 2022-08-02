from bin.validator import validator


def main(table1='table1.csv', table2='table2.csv', table3='table3.csv'):
    validator(table1, table2, table3)


if __name__ == "__main__":
    main()