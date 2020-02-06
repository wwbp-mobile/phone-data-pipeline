import argparse
import os
import sys
from datetime import datetime, timedelta

import pandas as pd
from sqlalchemy import create_engine

raw_data_tables = [
    'applications_foreground',
    'battery',
    'calls',
    'light',
    'locations',
    'messages',
    'screen'
]


def read_data_func(datastore, container):
    if datastore == 'sql':
        return lambda table_name: pd.read_sql('select * from {}'.format(table_name), container)
    elif datastore == 'csv':
        return lambda table_name: pd.read_csv(os.path.join(container, '{}.csv'.format(table_name)))
    else:
        raise TypeError('Unrecognized type: ' + str(datastore))


class TimeChunker(object):
    def __init__(self, earliest_timestamp, timespan):
        self.human_readable_timespan = timespan
        self.earliest_timestamp = earliest_timestamp

        self.next_time_chunk = self._time_info(timespan)

    @staticmethod
    def _time_info(timespan):
        def generate(func):
            def _inner_generate(earliest_dt):
                start_time, increment = func(earliest_dt)
                while True:
                    yield start_time, start_time + increment
                    start_time += increment
            return _inner_generate

        @generate
        def hourly(dt):
            return datetime(dt.year, dt.month, dt.day, hour=dt.hour).timestamp() * 1000, 3600000

        @generate
        def daily(dt):
            return datetime(dt.year, dt.month, dt.day).timestamp() * 1000, 86400000

        @generate
        def weekly(dt):
            first_day = dt - timedelta(days=(dt.weekday() + 1) % 7)
            return datetime(first_day.year, first_day.month, first_day.day).timestamp() * 1000, 604800000

        @generate
        def day_night(dt):
            morning = datetime(dt.year, dt.month, dt.day, hour=8)
            evening = datetime(dt.year, dt.month, dt.day, hour=20)
            previous_day = dt - timedelta(days=1)
            previous_evening = datetime(previous_day.year, previous_day.month, previous_day.day, hour=20)

            start_timestamp = previous_evening
            if evening < dt:
                start_timestamp = evening
            elif morning < dt:
                start_timestamp = morning
            return start_timestamp.timestamp() * 1000, 43200000

        timespan = str(timespan).lower()
        timespans = {
            'hourly': hourly,
            'daily': daily,
            'weekly': weekly,
            'day-night': day_night
        }

        if timespan not in timespans:
            raise ValueError('Invalid timespan: {}. Accepted: hourly, daily, weekly, day-night.'.format(timespan))

        return timespans[timespan]

    def chunks(self):
        earliest_datetime = datetime.fromtimestamp(self.earliest_timestamp / 1000)
        for start_time, end_time in self.next_time_chunk(earliest_datetime):
            yield start_time, end_time


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
    options.add_argument('--timespan', '-t', choices=['hourly', 'daily', 'weekly', 'day-night'], default='daily',
                         help='The timespan to use for aggregating raw data.')
    options.add_argument('--raw-data-tables', '-r', nargs='+', choices=raw_data_tables, default=raw_data_tables,
                         help='Specify raw data tables.')
    args = options.parse_args()

    # Setup connections to data sources and unify with a single read_data_table function
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
        read_data_table = read_data_func('sql', engine)
    else:  # CSV directory
        read_data_table = read_data_func('csv', args.csv_dir)

    for table in args.raw_data_tables:
        raw_data = read_data_table(table)

        earliest_timestamp = raw_data['timestamp'].min()
        latest_timestamp = raw_data['timestamp'].max()
        time_chunker = TimeChunker(earliest_timestamp, args.timespan)

        for start_time, end_time in time_chunker.chunks():
            if end_time >= latest_timestamp:
                break

            data_chunk = raw_data.loc[(raw_data['timestamp'] >= start_time) & (raw_data['timestamp'] < end_time), :]
