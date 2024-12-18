# %%
import pandas as pd
import redshift_connector
import re
from os import environ
from dotenv import load_dotenv
load_dotenv()


# %%
all_airports = pd.read_csv('https://www.fly.faa.gov/rmt/data_file/locid_db.csv',
                           usecols=['LocID', 'Facility', 'Location'])
all_airports = all_airports[all_airports['Location'].str.contains(r'\.', na=False)]
all_airports['airport_name'] = all_airports['Facility'].map(lambda x: x.split('.')[0])
all_airports['city'] = all_airports['Location'].map(lambda x: x.split('.')[0])
all_airports['state'] = all_airports['Location'].map(lambda x: x.split('.')[1].strip())
all_airports = all_airports.drop(["Facility", "Location"], axis=1)
all_airports

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

# %%
airports_to_add = all_airports.drop(all_airports[all_airports['airport_name'].isin(known_airports)].index)
airports_to_add

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

# %%
all_airports['city'] = all_airports['city'].map(cities_dict)
all_airports = all_airports.drop('state', axis=1)
all_airports

# %%
conn.close()


