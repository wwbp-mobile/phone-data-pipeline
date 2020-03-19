import pandas as pd


class DataTransformer(object):
    def __init__(self, data_table_chunker):
        self.data = data_table_chunker.table

        self._aggregations = []

    @property
    def table(self):
        """
        Lazily-applies the aggregation functions to each column in each chunk.
        :return: Pandas DataFrame object with aggregation functions applied to each
        """
        groups = self.data.groupby(['chunk_start', 'chunk_end'])

        aggregated_data_table = pd.DataFrame()
        for aggregation in self._aggregations:
            agg = groups.apply(aggregation[0], *aggregation[1:])
            aggregated_data_table = aggregated_data_table.merge(agg)
        return aggregated_data_table

    def register_aggregation(self, aggregation_func, *aggregation_func_args):
        """
        Set up aggregations to be applied to each chunk of the data table.
        :param aggregation_func: The aggregation function to apply to the specified column for each chunk.
        :return:
        """
        self._aggregations.append([aggregation_func] + list(aggregation_func_args))
