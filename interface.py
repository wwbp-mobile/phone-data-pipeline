import argparse
import os
import sys

from sqlalchemy import create_engine

from chunks import DataTableChunker
from constants import HOURLY, DAILY, WEEKLY, DAY_NIGHT, RAW_DATA_TABLES
from dataset import SQLConnection, CSVConnection, AWAREDataTable, StandardizedDataTable


if __name__ == '__main__':
    options = argparse.ArgumentParser(description='Extract features from raw cell phone data')
    data_args = options.add_mutually_exclusive_group(required=True)
    data_args.add_argument('--database', '-d', help='The database containing raw cell phone data.')
    data_args.add_argument('--csv-dir', help='The directory with CSV files containing raw cell phone data. '
                                             'Assumes the CSV files are named according to table name.')
    options.add_argument('--database-type', required='--database' in sys.argv, choices=['mysql', 'sqlite'],
                         default='sqlite', help='The type of database containing the raw cell phone data.')
    options.add_argument('--db-user', help='The database username to use for raw cell phone data.')
    options.add_argument('--db-pass', help='The database password to use for raw cell phone data.')
    options.add_argument('--db-host', default='localhost', help='The database host to use for raw cell phone data. '
                                                                'Default: localhost.')
    options.add_argument('--db-port', default='', help='The database port to use for raw cell phone data. '
                                                       'Default: 3306.')
    options.add_argument('--transformations-file', '-a', default='transformations.py',
                         help='The file containing the transformations to be applied. See the README for more '
                              'information.')
    options.add_argument('--timespan', '-t', choices=[HOURLY, DAILY, WEEKLY, DAY_NIGHT], default='daily',
                         help='The timespan to use for aggregating raw data.')
    options.add_argument('--raw-data-tables', '-r', nargs='+', choices=RAW_DATA_TABLES, default=RAW_DATA_TABLES,
                         help='Specify raw data tables.')
    options.add_argument('--framework', '-f', choices=['aware', 'pdk'], type=str.lower, default='aware',
                         help='The data collection framework used. Default: AWARE.')
    args = options.parse_args()

    # Setup connections to data sources and unify into DataTable interface
    if args.database:
        if args.database_type == 'mysql':
            if not args.db_user or not args.db_pass:
                options.error('You must specify a database username and password.')
            engine = create_engine('mysql://{user}:{passw}@{host}:{port}/{db}'.format(user=args.db_user,
                                                                                      passw=args.db_pass,
                                                                                      host=args.db_host,
                                                                                      port=args.db_port,
                                                                                      db=args.database))
        else:  # sqlite
            engine = create_engine('sqlite:///{db}'.format(db=os.path.abspath(args.database)))
        connection = SQLConnection(engine)
    else:  # CSV directory
        connection = CSVConnection(args.csv_dir)

    if args.framework == 'aware':
        DataTable = AWAREDataTable
    else:
        DataTable = StandardizedDataTable

    # Begin processing the data tables one at a time
    for table_name in args.raw_data_tables:
        data_table = DataTable(table_name)

        table_chunker = DataTableChunker(data_table, args.timespan)

        for data_table_chunk in table_chunker._chunk_table():
            pass
