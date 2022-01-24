import pandas as pd
import sqlalchemy

df = pd.read_csv("C://Repos/data-engineering-zoomcamp/week_1_basics_n_setup/2_docker_sql/test/yellow_tripdata_2021-01.csv",nrows=100)

# generate table schema
print(pd.io.sql.get_schema(df,name='yello_taxi_data'))

# Create connection to postgres
from sqlalchemy import create_engine 
engine = create_engine('postgresql://root:root@localhost:5432/ny_taxi')
engine.connect()

# generate table schema
print(pd.io.sql.get_schema(df,name='yello_taxi_data',con=engine))

# Chunk csv data 
df_iter = pd.read_csv("C://Repos/data-engineering-zoomcamp/week_1_basics_n_setup/2_docker_sql/test/yellow_tripdata_2021-01.csv",iterator=True,chunksize=100000)

# create data table in postgres (with no data inserted; only inserts 1st row)
df.head(n=0).to_sql(name='yellow_taxi-data',con=engine,if_exists='replace')

# insert data 
while True:
    df = next(df_iter)
    # fix data columns that are being read as text
    df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
    df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)
    
    df.to_sql(name='yellow_taxi-data',con=engine,if_exists='append')

