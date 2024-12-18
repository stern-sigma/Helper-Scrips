# %% [markdown]
# Imports

# %%
import pandas as pd
import redshift_connector
import re
from os import environ
from dotenv import load_dotenv
load_dotenv()


# %% [markdown]
# Import and load new airports data

# %%
all_airports = pd.read_csv('https://www.fly.faa.gov/rmt/data_file/locid_db.csv',
                           usecols=['LocID', 'Facility', 'Location'])
all_airports = all_airports[all_airports['Location'].str.contains(r'\.', na=False)]
all_airports['airport_name'] = all_airports['Facility'].map(lambda x: x.split('.')[0])
all_airports['city'] = all_airports['Location'].map(lambda x: x.split('.')[0])
all_airports['state'] = all_airports['Location'].map(lambda x: x.split('.')[1].strip())
all_airports = all_airports.drop(["Facility", "Location"], axis=1)
all_airports

# %% [markdown]
# Create redshift connection

# %%
import redshift_connector
from os import environ
from dotenv import load_dotenv
load_dotenv()
conn = redshift_connector.connect(
    user = environ['DB_USERNAME'],
    database = environ['DB_NAME'],
    password = environ['DB_PASSWORD'],
    port = environ['DB_PORT'],
    host = environ['DB_HOST']
)

# %% [markdown]
# Load a list of known airports

# %%
cur = conn.cursor()
cur.execute(
"""
SELECT 
    airport_name
FROM
    airport
;
"""
)
known_airports = {x[0] for x in cur.fetchall()}
cur.close()
known_airports

# %% [markdown]
# Remove known airports from the data to upload

# %%
airports_to_add = all_airports.drop(all_airports[all_airports['airport_name'].isin(known_airports)].index)
airports_to_add

# %% [markdown]
# Load a set of known cities

# %%
cur = conn.cursor()
cur.execute(
"""
SELECT 
    city_name
FROM
    city
;
"""
)
known_cities = {x[0] for x in cur.fetchall()}
cur.close()
known_cities

# %% [markdown]
# Convert state codes to state ids

# %%
cur = conn.cursor()
cur.execute(
"""
SELECT 
    state_code,
    state_id
FROM
    state
;
"""
)
state_dict = {x[0]: x[1] for x in cur.fetchall()}
cur.close()
airports_to_add['state'] = airports_to_add['state'].map(state_dict)
airports_to_add

# %% [markdown]
# Generate complete data for new cities

# %%
cities_to_add = airports_to_add[['city', 'state']]
to_drop = cities_to_add[cities_to_add['city'].isin(known_cities)].index
cities_to_add = cities_to_add.drop(to_drop)
cities_to_add = cities_to_add.drop_duplicates(ignore_index=True)
cur = conn.cursor()
cur.execute(
"""
SELECT 
    MAX(city_id)
FROM
    city
;
"""
)
max_ind = cur.fetchone()[0]

class Identity:
    def __init__(self, ind: int):
        self.ind = ind 
    def __iter__(self):
        return self
    def __next__(self):
        self.ind += 1
        return self.ind

curr_ind = Identity(max_ind)
cities_to_add = [(next(curr_ind), x[0], x[1]) for x in cities_to_add.itertuples(index=False)]
cities_to_add


# %% [markdown]
# Upload new cities

# %%
cur = conn.cursor()
cur.executemany(
"""
INSERT INTO
    city
    (city_id, city_name, state_id)
VALUES
    (%s, %s, %s)
""", param_sets=cities_to_add
)
conn.commit()
cur.close()

# %% [markdown]
# Create a cities id dictionary for mapping city names

# %%
cur = conn.cursor()
cur.execute(
"""
SELECT 
    city_name,
    city_id
FROM
    city
;
"""
)
cities_dict = {x[0]:x[1] for x in cur.fetchall()}
cur.close()
cities_dict

# %% [markdown]
# Convert city names to city ids and drop redundant state column

# %%
airports_to_add['city'] = airports_to_add['city'].map(cities_dict)
airports_to_add = airports_to_add.drop('state', axis=1)
airports_to_add

# %% [markdown]
# Filter out known airports from data to upload

# %%
cur = conn.cursor()
cur.execute(
"""
SELECT 
    airport_id
FROM
    airport
;
"""
)
known_airports = {x[0] for x in cur.fetchall()}
airports_to_drop = airports_to_add[airports_to_add["LocID"].isin(known_airports)].index
airports_to_add = airports_to_add.drop(airports_to_drop)
cur.close()
airports_to_add

# %% [markdown]
# Upload new airports

# %%
cur = conn.cursor()
cur.execute('SELECT MAX(airport_id) FROM airport;')
airport_index = Identity(cur.fetchone()[0])
new_airports = [(next(airport_index), x[0], x[1], x[2]) 
                for x in airports_to_add.itertuples(index=False)
                if len(x[0]) <= 5]
cur.executemany(
""" 
INSERT INTO 
    airport
    (airport_id, airport_code, airport_name, city_id)
VALUES
    (%s, %s, %s, %s)
;
""", param_sets=new_airports
)
conn.commit()
cur.close()

# %% [markdown]
# Teardown

# %%
conn.close()


