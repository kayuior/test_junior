import datetime

from ETL import ETL

if __name__ == "__main__":
    month_list = [datetime.date(2016, 1, 1),datetime.date(2016, 2, 1),datetime.date(2016, 3, 1)]
    for month in month_list:
        ETL(month, datetime.date(2016, 1, 1)).run()
