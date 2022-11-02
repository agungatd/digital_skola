import pandas as pd
import psycopg2
import sqlalchemy as sa

data_property = pd.read_csv('dataset/TR_PropertyInfo.csv')
data_user = pd.read_csv('dataset/TR_UserInfo.csv')

cols_property = {'Prop ID': 'prop_id', 'PropertyCity': 'property_city', 'PropertyState': 'property_state'}
cols_user = {'UserID': 'user_id', 'UserSex': 'user_sex', 'UserDevice': 'user_device'}

data_property = data_property.rename(columns=cols_property)
data_user = data_user.rename(columns=cols_user)


database='postgres'
user='postgres'
password='postgres'
host='localhost'
port=5432

conn = sa.create_engine(f'postgresql://{user}:{password}@{host}:{port}/{database}')

data_property.to_sql('property', con=conn, index=False, if_exists='replace')
data_user.to_sql('user', con=conn, index=False, if_exists='replace')