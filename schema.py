# -*- coding: utf-8 -*-
"""Describes raw data table schema in terms of SQL Alchemy MetaData objects.

The `create_schema()` function can be used to generate raw data table formats, 
for easy loading/dumping to SQLite.

TODO:
    * review schema for any missing attributes/indices 
    * unit tests for schema verification (?)
"""
from sqlalchemy import Float, String, Boolean, Integer, create_engine
from sqlalchemy.schema import Column, MetaData, Table, Sequence, Index

def create_schema():
    """Constructs SQL Alchemy MetaData and defines raw data table format.

    See the SQL Alchemy docs for MetaData attributes and manipulation: 
        https://docs.sqlalchemy.org/en/13/core/metadata.html

    Returns:
        sqlalchemy.MetaData: MetaData object ready for inspection or connection to database engine.

    Examples:
        Create an in-memory sqlite db and list table names
        
        >>> engine = create_engine('sqlite:///:memory:')    
        >>> schema = create_schema()
        >>> schema.create_all(engine)
        >>> for t in schema.sorted_tables: 
        ...    print(t.name)
        applications_foreground
        battery
        calls
        light
        locations
        messages
        screen

    """

    metadata = MetaData()

    app_table = Table('applications_foreground', metadata,
        Column('_id', Integer, primary_key=True, autoincrement=True),
        Column('timestamp', Float, default=0), 
        Column('timezone_offset', Float, default=0),
        Column('device_id', String, default=''),
        Column('package_name', String),
        Column('screen_active', Boolean)) # TODO: not in AWARE spec, remove?
        
    bat_table = Table('battery', metadata,
        Column('_id', Integer, primary_key=True, autoincrement=True),
        Column('timestamp', Float, default=0), 
        Column('timezone_offset', Float, default=0),
        Column('device_id', String, default=''),
        Column('battery_level', Integer, default=0),
        Column('battery_level', Integer, default=0))
    
    cal_table = Table('calls', metadata,
        Column('_id', Integer, primary_key=True, autoincrement=True),
        Column('timestamp', Float, default=0), 
        Column('timezone_offset', Float, default=0),
        Column('device_id', String, default=''),
        Column('call_type', Integer, default=0),
        Column('call_duration', Integer, default=0),
        Column('trace', String))

    lig_table = Table('light', metadata,
        Column('_id', Integer, primary_key=True, autoincrement=True),
        Column('timestamp', Float, default=0), 
        Column('timezone_offset', Float, default=0),
        Column('device_id', String, default=''),
        Column('double_light_lux', Float, default=0),
        Column('accuracy', Integer, default=0))

    loc_table = Table('locations', metadata,
        Column('_id', Integer, primary_key=True, autoincrement=True),
        Column('timestamp', Float, default=0), 
        Column('timezone_offset', Float, default=0),
        Column('device_id', String, default=''),
        Column('double_latitude', Float, default=0),
        Column('double_longitude', Float, default=0),
        Column('double_speed', Float, default=0),
        Column('accuracy', Float, default=0))

    mes_table = Table('messages', metadata,
        Column('_id', Integer, primary_key=True, autoincrement=True),
        Column('timestamp', Float, default=0), 
        Column('timezone_offset', Float, default=0),
        Column('device_id', String, default=''),
        Column('message_type', Integer, default=0),
        Column('trace', String))

    scr_table = Table('screen', metadata,
        Column('_id', Integer, primary_key=True, autoincrement=True),
        Column('timestamp', Float, default=0), 
        Column('timezone_offset', Float, default=0),
        Column('device_id', String, default=''),
        Column('screen_status', Integer, default=0))

    return metadata

    
if __name__ == '__main__':
    engine = create_engine('sqlite:///:memory:')    

    schema = create_schema()
    schema.create_all(engine)
    for t in schema.sorted_tables:
        print(t.name)

    msg = schema.tables['messages']
    
    # insert into the messages table
    ins = msg.insert().values(
        timestamp=1234,
        timezone_offset=-6000,
        device_id='1',
        message_type=0,
        trace='hash'
    )

    # execute the statement
    conn = engine.connect()
    conn.execute(ins)

    # run docstring tests
    import doctest
    doctest.testmod()