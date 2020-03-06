from datetime import datetime, timedelta

from constants import HOURLY, DAILY, WEEKLY, DAY_NIGHT


class DataTableChunk(object):
    def __init__(self, data_chunk, chunk_start, chunk_end):
        """
        :param data_chunk: A pandas DataFrame view containing rows within a timespan.
        :param chunk_start: The chunk start time in milliseconds.
        :param chunk_end: The chunk end time in milliseconds.
        """
        self.data = data_chunk
        self.start = chunk_start
        self.end = chunk_end


class DataTableChunker(object):
    def __init__(self, data_table, timespan):
        self.human_readable_timespan = timespan
        self.earliest_timestamp = data_table.table['timestamp'].min()
        self.latest_timestamp = data_table.table['timestamp'].max()
        self.next_time_chunk = self._time_info(timespan)

        self._data_table = data_table

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
            HOURLY: hourly,
            DAILY: daily,
            WEEKLY: weekly,
            DAY_NIGHT: day_night
        }

        if timespan not in timespans:
            raise ValueError('Invalid timespan: {}. Accepted: {}.'.format(timespan, ', '.join(timespans.keys())))

        return timespans[timespan]

    def chunks(self):
        earliest_datetime = datetime.fromtimestamp(self.earliest_timestamp / 1000)

        for start_time, end_time in self.next_time_chunk(earliest_datetime):
            if start_time >= self.latest_timestamp:
                break

            data_chunk = self._data_table.table.loc[
                         (self._data_table.table['timestamp'] >= start_time) &
                         (self._data_table.table['timestamp'] < end_time), :]
            yield DataTableChunk(data_chunk, start_time, end_time)
