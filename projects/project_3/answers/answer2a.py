# %%
import json
import pandas as pd
import psycopg2 as pg
from sqlalchemy import create_engine
from zipfile import ZipFile
from datetime import date, datetime
import configparser

# %%
config = configparser.ConfigParser()
config.read('config.ini')

# %%
data = pd.read_json('../sql/schemas/user_address.json')

# %%
data.drop(['is_primary_key', 'is_unique'], axis=1, inplace=True)

# %%
schema_list = []
for i, row in data.iterrows():
    row = tuple(row.astype("string"))
    row_str = ' '.join(list(row))
    schema_list.append(row_str)

query = """
CREATE TABLE if not exists user_address_2018_snapshots (
"""
for i, schema in enumerate(schema_list):
    query += schema
    if i < len(schema_list) - 1:
        query += ','
query += ')'
print(query)

# %%
database = config['postgres']['database']
user = config['postgres']['user']
password = config['postgres']['password']
host = config['postgres']['host']
port = config['postgres']['port']

#Init Posgres conn
conn = pg.connect(database=database,
                  user=user,
                  password=password,
                  host=host,
                  port=port)

conn.autocommit=True
cursor=conn.cursor()

# %%
# Execute DDL
try:
    cursor.execute(query)
except Exception as e:
    print(e)

# %%
# read zip file
zf = ZipFile('../temp/dataset-small.zip')
df = pd.read_csv(
    zf.open('dataset-small.csv'), 
    header=None, names=data.column_name.tolist(), 
    parse_dates=['created_at'])
df.created_at = pd.to_datetime(df.created_at).dt.tz_localize(None)
df.head()

# %%
# filter data created at '2018-02-01' until '2018-12-31'
df_filtered = df[(df.created_at >= datetime(2018,2,1)) & (df.created_at <= datetime(2018,12,31))]
df_filtered.head()

# %%
#create engine
engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{database}')

# %%
table_name = config['DEFAULT']['table_name']
try:
    df_filtered.to_sql(table_name, engine, if_exists='replace', index=False)

except Exception as e:
    print(e)

print(f'Total inserted rows: {len(df_filtered)}')
print(f'Inital created_at: {df_filtered.created_at.min()}')
print(f'Last created_at: {df_filtered.created_at.max()}')

cursor.execute(open('../sql/queries/result_ingestion_user_address.sql', 'r').read())
result = cursor.fetchall()
print(result)

# %%



