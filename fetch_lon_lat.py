import datetime
def curr_timestamp():
    current_datetime = datetime.datetime.now()
    formatted_datetime = current_datetime.strftime("%Y-%m-%d_%H-%M-%S")
    return formatted_datetime

import pandas as pd

archives_dir = './Archive/'
file = 'year_state_county_yield.csv'

df = pd.read_csv(archives_dir + file)
print('number of rows in csv cleaned for ML: ', len(df))
print()
print(df.head())


df1 = df[['state_name','county_name']].drop_duplicates()
print('\nNumber of state-county pairs is: ', len(df1))

# including the "or" (i.e., "|") so that I can this cell is eidenpotent
index = df.index[(df['county_name'] == 'DU PAGE') | (df['county_name'] == 'DUPAGE')].tolist()
print(index)
for ind in index:
    df.at[ind, 'county_name'] = 'DUPAGE'
    print(df.at[ind, 'county_name'])

index1 = df1.index[(df1['county_name'] == 'DU PAGE') | (df1['county_name'] == 'DUPAGE')].tolist()
print(index1)
for ind in index1:
    df1.at[ind, 'county_name'] = 'DUPAGE'
    print(df1.at[ind, 'county_name'])

print('\nNumber of state-county pairs is: ', len(df1))

from geopy.geocoders import Nominatim

# Geocoding function to retrieve coordinates for a county
def geocode_county(state, county):
    geolocator = Nominatim(user_agent="county_geocoder")
    location = geolocator.geocode(county + ", " + state + ", USA")
    if location:
        return location.longitude, location.latitude
    else:
        print('no lat-lon found for ', state, county)
        return None, None

df1['lon'] = df1.apply(lambda x: geocode_county(x['state_name'], x['county_name'])[0], axis=1)
df1['lat'] = df1.apply(lambda x: geocode_county(x['state_name'], x['county_name'])[1], axis=1)

print(df1.head())
print()

print('lon-lat for ILLINOIS-BUREAU is: ', geocode_county('ILLINOIS', 'BUREAU'))

filename = 'state_county_lon_lat.csv'
df1.to_csv(archives_dir + filename, index=False)
print('wrote file: ', archives_dir + filename)