def mean(table, column):
    return table.groupby('device_id')[column].mean()


def most_common(table, column):
    return table.groupby('device_id')[column].mode().iloc[0]


transformations = {
    'battery': {
        'battery_level': mean,
        'battery_adapter': most_common,
    }
}
