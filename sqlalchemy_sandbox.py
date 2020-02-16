# %% Imports
import pandas as pd
from sqlalchemy import Float, String, Boolean, Integer, create_engine
from sqlalchemy.schema import Column, MetaData, Table, Sequence, Index

# %% Define sample schema for applications_foreground
metadata = MetaData()

afg_table = Table('applications_foreground', metadata,
    Column('_id', Integer, primary_key=True, autoincrement=True),
    Column('timestamp', Float, default=0), 
    Column('device_id', String(150), default=''),
    Column('package_name', String),
    Column('screen_active', Boolean),
    Index('time_index', 'timestamp', 'device_id'))

cols = ['timestamp', 'device_id', 'package_name', 'screen_active']

# create DB in memory and load table
engine = create_engine('sqlite:///:memory:')

# sqlite file example
#engine = create_engine('sqlite:///foo.db')

afg_table.create(engine)
# if the metadata schema includes multiple tables
#metadata.create_all(engine)

# %% load sample fga

test_id = '00746649'
afg_path = "/data/tliu/wk4_ls_data/pdk-foreground-application/{}.df"

afg_df = pd.read_pickle(afg_path.format(test_id))

afg_df.head()

# %% convert cols and import into sqlalchemy DB

afg_df['device_id'] = afg_df['pid'].astype(str)
afg_df['package_name'] = afg_df['application']

# commits to the database
afg_df[cols].to_sql('applications_foreground', con=engine, if_exists='append', index=False)

# %% load the sqlite table into Pandas frame
sql_df = pd.read_sql_table("applications_foreground", con=engine)
sql_df.head()
