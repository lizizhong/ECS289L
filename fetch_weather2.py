# This useful if I want to give unique names to directories or files
import datetime

import requests


def curr_timestamp():
    current_datetime = datetime.datetime.now()
    formatted_datetime = current_datetime.strftime("%Y-%m-%d_%H-%M-%S")
    return formatted_datetime

import pandas as pd

archive_dir = './Archive/'
yscy_file = 'year_state_county_yield.csv'
scll_file = 'state_county_lon_lat.csv'

df_yscy = pd.read_csv(archive_dir + yscy_file)
df_scll = pd.read_csv(archive_dir + scll_file)

# Recall that when getting the lon-lat, I changed the name of "DU PAGE, ILLINOIS" to "DUPAGE, ILLINOIS"
# I will make the same name substitution in df_yscy
index_list = df_yscy.index[(df_yscy['county_name'] == 'DU PAGE') | (df_yscy['county_name'] == 'DUPAGE')].tolist()
print(index_list)
for i in index_list:
    df_yscy.at[i, 'county_name'] = 'DUPAGE'
    print(df_yscy.at[i, 'county_name'])


print(len(df_yscy), len(df_scll))

df_yscyll = pd.merge(df_yscy,df_scll, on=['state_name','county_name'],how='left')

print()
print(len(df_yscyll))
print()
# print(df_yscyll.head(30))

# sanity check - that lon/lat's in new df correspond to the lon/lat's from table state_county_lon_lat.csv
print(df_yscyll[df_yscyll['year'] == 2022].head(10))
print(df_scll.head(10))

# checking on the DU PAGE county entries
print()
print(df_yscyll.iloc[279:284].head())

yscyll_filename = 'year_state_county_yield_lon_lat.csv'
df_yscyll.to_csv(archive_dir + yscyll_filename, index=False)

print('wrote file: ', archive_dir + yscyll_filename)


# setting up a URL template for making requests to NASA POWER

# growing season from April to October


import json

# see https://gist.github.com/abelcallejo/d68e70f43ffa1c8c9f6b5e93010704b8
#   for available parameters
# I will focus on the following parameters
weather_params = ['T2M_MAX','T2M_MIN', 'PRECTOTCORR', 'GWETROOT', 'EVPTRNS', 'ALLSKY_SFC_PAR_TOT'
                  ] #
'''
   T2M_MAX: The maximum hourly air (dry bulb) temperature at 2 meters above the surface of the 
             earth in the period of interest.
   T2M_MIN: The minimum hourly air (dry bulb) temperature at 2 meters above the surface of the 
            earth in the period of interest.
   PRECTOTCORR: The bias corrected average of total precipitation at the surface of the earth 
                in water mass (includes water content in snow)
   EVPTRNS: The evapotranspiration energy flux at the surface of the earth
   ALLSKY_SFC_PAR_TOT: The total Photosynthetically Active Radiation (PAR) incident 
         on a horizontal plane at the surface of the earth under all sky conditions
'''

# Now setting up parameterized URLs which will pull weather data,
# focused on growing season, which is April to October
# following
#     https://power.larc.nasa.gov/docs/tutorials/service-data-request/api/
base_url = r"https://power.larc.nasa.gov/api/temporal/daily/point?"
base_url += 'parameters=T2M_MAX,T2M_MIN,PRECTOTCORR,GWETROOT,EVPTRNS,ALLSKY_SFC_PAR_TOT&'
base_url += 'community=RE&longitude={longitude}&latitude={latitude}&start={year}0101&end={year}1231&format=JSON'
# print(base_url)

import json
import requests


def fetch_weather_county_year(year, state, county):
    row = df_yscyll.loc[(df_yscyll['state_name'] == state) & \
                        (df_yscyll['county_name'] == county) & \
                        (df_yscyll['year'] == year)]
    # print(row)
    # print(type(row))
    lon = row.iloc[0]['lon']
    lat = row.iloc[0]['lat']
    # print(lon, lat)
    api_request_url = base_url.format(longitude=lon, latitude=lat, year=str(year))

    # this api request returns a json file
    response = requests.get(url=api_request_url, verify=True, timeout=30.00)
    # print(response.status_code)
    content = json.loads(response.content.decode('utf-8'))

    # print('\nType of content object is: ', type(content))
    # print(json.dumps(content, indent=4))

    # print('\nKeys of content dictionary are: \n', content.keys())
    # print('\nKeys of "properties" sub-dictionary are: \n', content['properties'].keys())
    # print('\nKeys of "parameter" sub-dictionary are: \n', content['properties']['parameter'].keys())
    # print()

    weather = content['properties']['parameter']

    df = pd.DataFrame(weather)
    return df


# sanity check
# df = fetch_weather_county_year(2022, 'ILLINOIS', 'LEE')

# examining the output
# print(len(df))
# print()
# print(df.head())

# w_df will be a dictionary of df's, each holding weather info for
#    one year-state-county triple
# The dictionary keys will be the index values of df_yscyll that we
#    built above (also archived in year_state_county_yield_lon_lat.csv)
w_df = {}

# will archive each w_df[i] value into a csv as we go along, for 2 reasons:
#   - because this takes forever to run
#   - network connectivity or other errors in middle of run may abort the process
out_dir = archive_dir + 'WEATHER/'
filename = r'weather-data-for-index__{index}.csv'

starttime = datetime.datetime.now().strftime("%Y-%m-% %H:%M:%S")

# for i in range(0,len(df_yscyll)):
# for i in range(278,280):    # had to fix issue of DU PAGE county...
# for i in range(280,len(df_yscyll)):
# for i in range(1779,1780):  # when running big loop it failed at 1779; but worked in this run; network issue?
# for i in range(1779, len(df_yscyll)): # blocked at 2534
# for i in range(2534,2535):    # when running big loop it failed at 1779; but worked in this run; network issue?
# for i in range(0, len(df_yscyll)):
for i in range(1048, len(df_yscyll)):
    row = df_yscyll.iloc[i]
    w_df[i] = fetch_weather_county_year(row['year'], row['state_name'], row['county_name'])
    outfilename = out_dir + filename.format(index=str(i).zfill(4))
    w_df[i].to_csv(outfilename)
    # adding this to get a feeling of forward progress
    if i % 10 == 0:
        print('\nFinished work on index: ', i, '     at time: ', datetime.datetime.now().strftime("%Y-%m-% %H:%M:%S"))
        print('   This involved fetching weather data for the following row:')
        print(row['year'],row['state_name'],row['county_name'], row['lon'],row['lat'])
        print('Wrote file: ', outfilename)

'''
# sanity check
print()
index = 2
print(r'The contents of yscyll for index {index} is:'.format(index=index), '\n')
print(df_yscyll.iloc[index])
print()
print(r'The head of the weather data for index {index} is:'.format(index=index), '\n')
print(w_df[index].head(10))
'''

endtime = datetime.datetime.now().strftime("%Y-%m-% %H:%M:%S")
print('start and end times were: ', starttime, endtime)