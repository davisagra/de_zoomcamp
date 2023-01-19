#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
from sqlalchemy import create_engine
engine = create_engine('postgresql://root:root@localhost:5432/ny_taxi')
engine.connect()


# In[2]:


query = """
SELECT count(1)
FROM green_taxi_data
where date(lpep_pickup_datetime) = '2019-01-15' and date(lpep_dropoff_datetime) = '2019-01-15'
"""
pd.read_sql(query, con=engine)


# In[3]:


query = """
SELECT date(lpep_pickup_datetime), max(((lpep_dropoff_datetime) - (lpep_pickup_datetime)))
FROM green_taxi_data
group by date(lpep_pickup_datetime)
order by max desc
"""
pd.read_sql(query, con=engine)


# In[5]:


query = """
SELECT count(1)
FROM green_taxi_data
where date(lpep_pickup_datetime) = '2019-01-01' and (passenger_count = 2)
"""
pd.read_sql(query, con=engine)


# In[4]:


query2 = """
SELECT count(1)
FROM green_taxi_data
where date(lpep_pickup_datetime) = '2019-01-01' and (passenger_count = 3)
"""
pd.read_sql(query2, con=engine)


# In[6]:


query = """
SELECT max(tip_amount) max, c."Zone" as drop_up_zone
FROM green_taxi_data a 
inner join zones b
on a."PULocationID"=b."LocationID"
inner join zones c
on a."DOLocationID"=c."LocationID"
where b."Zone"='Astoria'
group by c."Zone"
order by max desc
"""
pd.read_sql(query, con=engine)

