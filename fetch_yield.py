# This useful if I want to give unique names to directories or files
import datetime
def curr_timestamp():
    current_datetime = datetime.datetime.now()
    formatted_datetime = current_datetime.strftime("%Y-%m-%d_%H-%M-%S")
    return formatted_datetime


# from https://towardsdatascience.com/harvest-and-analyze-agricultural-data-with-the-usda-nass-api-python-and-tableau-a6af374b8138
# with edits

#   Name:           c_usda_quick_stats.py
#   Author:         Randy Runtsch
#   Date:           March 29, 2022
#   Project:        Query USDA QuickStats API
#   Author:         Randall P. Runtsch
#
#   Description:    Query the USDA QuickStats api_GET API with a specified set of
#                   parameters. Write the retrieved data, in CSV format, to a file.
#
#   See Quick Stats (NASS) API user guide:  https://quickstats.nass.usda.gov/api
#   Request a QuickStats API key here:      https://quickstats.nass.usda.gov/api#param_define
#
#   Attribution: This product uses the NASS API but is not endorsed or certified by NASS.
#
#   Changes
#

import urllib.request
from urllib.error import HTTPError
from requests.utils import requote_uri
import requests

# Retrieve NASS API key from environment variables (you have to get your own)
import os

my_NASS_API_key = "084DA488-C8DC-3EAD-BB94-ECABA3C0470C"


class c_usda_quick_stats:

    def __init__(self, my_NASS_API_key):

        # Set the USDA QuickStats API key, API base URL, and output file path where CSV files will be written.

        # self.api_key = 'PASTE_YOUR_API_KEY_HERE'
        self.api_key = my_NASS_API_key

        self.base_url_api_get = 'http://quickstats.nass.usda.gov/api/api_GET/?key=' \
                                + self.api_key + '&'

    def get_data(self, parameters, file_path, file_name):

        # Call the api_GET api with the specified parameters.
        # Write the CSV data to the specified output file.

        # Create the full URL and retrieve the data from the Quick Stats server.

        full_url = self.base_url_api_get + parameters
        print(full_url)

        try:
            s_result = urllib.request.urlopen(full_url)
            # print(type(s_result))
            print(s_result.status, s_result.reason)
            # print(s_result.status_code)
            s_text = s_result.read().decode('utf-8')

            # Create the output file and write the CSV data records to the file.

            s_file_name = file_path + file_name
            o_file = open(s_file_name, "w", encoding="utf8")
            o_file.write(s_text)
            o_file.close()
        except HTTPError as error:
            print(error.code, error.reason)
        except requests.exceptions.RequestException as e:
            print(f"An error occurred while fetching the data: {e}")
        except ValueError as e:
            print(f"Failed to parse the response data: {e}")
        except:
            print(f"Failed because of unknown exception; perhaps the USDA NASS site is down")

# from https://towardsdatascience.com/harvest-and-analyze-agricultural-data-with-the-usda-nass-api-python-and-tableau-a6af374b8138
# with edits

#   Date:           March 29, 2022
#   Project:        Program controller to query USDA QuickStats API
#   Author:         Randall P. Runtsch
#
#   Description:    Create an instance of the c_usda_quick_stats class. Call it with
#                   the desired search parameter and output file name.
#
#   Attribution: This product uses the NASS API but is not endorsed or certified by NASS.
#
#   Changes
#

import sys
import urllib.parse

output_dir = './OUTPUTS/'

# Create a string with search parameters, then create an instance of
# the c_usda_quick_stats class and use that to fetch data from QuickStats
# and write it to a file



# the QuickStats site is very senstivite to how the full URL is built up.
# For example, the following spec for the parameters works
# But if you replace the line "'&unit_desc=ACRES' + \" with
# the line "'&' + urllib.parse.quote('unit_desc-ACRES')"
# then the site responds saying that you have exceeded the 50,000 record limit for one query

parameters =    'source_desc=SURVEY' +  \
                '&' + urllib.parse.quote('sector_desc=FARMS & LANDS & ASSETS') + \
                '&' + urllib.parse.quote('commodity_desc=FARM OPERATIONS') + \
                '&' + urllib.parse.quote('statisticcat_desc=AREA OPERATED') + \
                '&unit_desc=ACRES' + \
                '&freq_desc=ANNUAL' + \
                '&reference_period_desc=YEAR' + \
                '&year__GE=1997' + \
                '&agg_level_desc=NATIONAL' + \
                '&' + urllib.parse.quote('state_name=US TOTAL') + \
                '&format=CSV'

stats = c_usda_quick_stats(my_NASS_API_key)

# Including curr_timestamp() into file name to keep outputs separated during development/exploration
s_json = stats.get_data(parameters, output_dir, 'national_farm_survey_acres_ge_1997_' + curr_timestamp() + '.csv')


import sys
import urllib.parse

# Create a string with search parameters, then create an instance of
# the c_usda_quick_stats class and use that to fetch data from QuickStats
# and write it to a file

# It took a while to get the parameter names just right...
#   The parameters names are listed in
#      https://quickstats.nass.usda.gov/param_define
#   (some additional resources in https://quickstats.nass.usda.gov/tutorials)
#   Also, look at the column names that show up in the csv files that you get back
parameters =    'source_desc=SURVEY' +  \
                '&sector_desc=CROPS' + \
                '&' + urllib.parse.quote('group_desc=FIELD CROPS') + \
                '&commodity_desc=WHEAT' + \
                '&statisticcat_desc=YIELD' + \
                '&geographic_level=STATE' + \
                '&agg_level_desc=COUNTY' + \
                '&state_name=KANSAS' + \
                '&state_name=MONTANA' + \
                '&state_name=WASHINGTON' + \
                '&year__GE=1993' + \
                '&year__LE=2007' + \
                '&format=CSV'
# '&state_name=OKLAHOMA' + \
# '&state_name=TEXAS' + \
    # '&agg_level_desc=COUNTY' + \
stats = c_usda_quick_stats(my_NASS_API_key)

# holding this timestamp; we may used it to import the created csv file
latest_curr_timestamp = curr_timestamp()
filename = 'wheat_yield_data__' + latest_curr_timestamp + '.csv'

# Including curr_timestamp() into file name to keep outputs separated during development/exploration
stats.get_data(parameters, output_dir, 'wheat_yield_data__' + latest_curr_timestamp + '.csv')


import pandas as pd

df = pd.read_csv(output_dir + filename)
# print(df.head())

df1 = df[['short_desc']].drop_duplicates()
print(df1.head(10))
print()

# keep only records about full yield
print(df)
df = df[df['short_desc'] == "WHEAT - YIELD, MEASURED IN BU / ACRE"]
print(len(df))
# 10295

print()

# found some bad_county_names by visual inspection of the csv
bad_county_names = ['OTHER COUNTIES', 'OTHER (COMBINED) COUNTIES']
df = df[~df.county_name.isin(bad_county_names)]

print(len(df))
# 9952

print()

df2 = df[['state_name','county_name']].drop_duplicates()
print(len(df2))
# 559

# Note: using SQL I found that of the 559 state-county pairs total:
#          212 state-county pairs have data for all 20 years
#          347 state-county pairs have data for < 20 years
#
#          486 have year 2022
#          418 have year 2021
#          514 have year 2020
# I will live with that

# cleaning up a column name
df = df.rename(columns={'Value': 'yield'})

output_file = 'repaired_yield__' + curr_timestamp() + '.csv'

df.to_csv(output_dir + output_file, index=False)

# I imported this table into postgres so that I could use SQL ...


import shutil

output_dir = './OUTPUTS/'
archives_dir = './Archive/'
src_file = output_file # from preceding cell
tgt_file = 'wheat_yield_data2.csv'

shutil.copyfile(output_dir + src_file, archives_dir + tgt_file)

import pandas as pd

tgt_file = 'wheat_yield_data2.csv'

df = pd.read_csv(archives_dir + tgt_file)
# print(df.head())

cols_to_keep = ['year','state_name','county_name','yield']
dfml = df[cols_to_keep]

print(dfml.head())
print()
print(dfml.shape[0])
# Note: this particular df has 9952 rows

# checking there are no null values for 'yield':
print(dfml[dfml['yield'].isnull()].head())

tgt_file_01 = 'd2'
dfml.to_csv(archives_dir + tgt_file_01, index=False)
print('\nwrote file ', archives_dir + tgt_file_01)