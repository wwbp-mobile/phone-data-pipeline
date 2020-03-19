import os
from abc import ABC, abstractmethod
from datetime import datetime

import pandas as pd
import pytz


class Connection(ABC):
    """A connection provides access to a set of raw data tables."""
    def __init__(self, container):
        self.container = container

    @abstractmethod
    def read_data_table(self, table_name):
        pass


class SQLConnection(Connection):
    def __init__(self, container):
        super().__init__(container)

    def read_data_table(self, table_name):
        return pd.read_sql('select * from {}'.format(table_name), self.container)


class CSVConnection(Connection):
    def __init__(self, container, sep=','):
        self._sep = sep
        super().__init__(container)

    def read_data_table(self, table_name):
        return pd.read_csv(os.path.join(self.container, '{}.csv'.format(table_name)), sep=self._sep)


class DataTable(ABC):
    """Abstract class representing a data table."""
    def __init__(self, connection, table_name):
        self.table_name = table_name

        self._connection = connection
        self._raw_table = None
        self._standard_table = None

    @property
    def raw_table(self):
        """Lazy-loads raw data tables."""
        if self._raw_table is None:
            self._raw_table = self._connection.read_data_table(self.table_name)
        return self._raw_table

    @property
    def table(self):
        """Lazily-processes the raw data table into a standardized format."""
        if self._standard_table is None:
            self._standard_table = self._standardized_table()
        return self._standard_table

    @abstractmethod
    def _standardized_table(self):
        pass


class AWAREDataTable(DataTable):
    def __init__(self, connection, table_name):
        super().__init__(connection, table_name)

    def _standardized_table(self):
        # Load the raw timezone table. There is probably a cleaner way to do this.
        timezone_table = self._connection.read_data_table('timezone')

        # Restructure the timezone table to have a beginning (inclusive) and ending (exclusive) timestamp.
        # The ending timestamp is the next timestamp found for that device. If there is no next timestamp,
        # use the latest timestamp from the table to be processed + 1.
        max_timestamp = self.raw_table['timestamp'].max() + 1
        timezone_table['end_timestamp'] = timezone_table \
            .sort_values(['device_id', 'timestamp']) \
            .groupby('device_id')['timestamp'] \
            .shift(-1) \
            .rename(columns={'timestamp': 'start_timestamp'}) \
            .fillna(max_timestamp) \
            .drop('_id')

        # Assumes we have an initial timezone at recruitment time, in which case all values in the sensor table
        # should occur between the earliest timezone timestamp and the latest sensor table timestamp (which is also the
        # latest timezone stamp).
        table = self.raw_table.merge(timezone_table, on='device_id')
        table = table[(table['timestamp'] >= table['start_timestamp']) & (table['timestamp'] < table['end_timestamp'])]
        table['time_offset'] = table.apply(lambda r: pytz
                                           .timezone(r['timezone'])
                                           .utcoffset(datetime.fromtimestamp(r['timestamp'] // 1000)),
                                           axis='columns')
        return table


class StandardizedDataTable(DataTable):
    """For datasets that have already been standardized."""
    def __init__(self, connection, table_name):
        super().__init__(connection, table_name)

    @property
    def table(self):
        """Overrides so that we only keep a single copy of the table in memory (in self._raw_table)."""
        return self._standardized_table()

    def _standardized_table(self):
        return self.raw_table
