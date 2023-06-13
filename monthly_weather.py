import pandas as pd

archive_dir = './Archive/'
yscyll_filename = 'year_state_county_yield_lon_lat.csv'

weather_dir = archive_dir + 'WEATHER/'
wdtemplate = r'weather-data-for-index__{padded}.csv'

df_yscyll = pd.read_csv(archive_dir + yscyll_filename)
print(df_yscyll.shape)
print()

w_df = {}
for i in range(0, 6094):
    padded = str(i).zfill(4)
    w_df[i] = pd.read_csv(weather_dir + wdtemplate.format(padded=padded))
    # Want to have a name for the index of my dataframe
    w_df[i].rename(columns={'Unnamed: 0': 'date'},
                   inplace=True)
    # w_df[i] = w_df[i].rename_axis(index='DATE')

print()
print(w_df[4].shape)
print(w_df[4].head())


# takes as input a dataframe whose index field is called "date" and
#   holds 8-character dates, and with columns
#   ['T2M_MAX', 'T2M_MIN', 'PRECTOTCORR', 'GWETROOT', 'EVPTRNS', 'ALLSKY_SFC_PAR_TOT']
# produces dataframe with same shape, but the values are grouped by MONTH,
#   with a particular aggregation used for each column

def create_monthly_df(df):
    df1 = df.copy()
    # convert index to datetime format
    df1.index = pd.to_datetime(df['date'], format='%Y%m%d')
    # use 'M' for monthly, use 'W' for weekly
    df1_monthly = df1.resample('M').agg({'T2M_MAX': 'mean',
                                         'T2M_MIN': 'mean',
                                         'PRECTOTCORR': 'sum',
                                         'GWETROOT': 'mean',
                                         'EVPTRNS': 'mean',
                                         'ALLSKY_SFC_PAR_TOT': 'sum'})

    # convert index back to string format YYYYMM
    df1_monthly.index = df1_monthly.index.strftime('%Y%m%a')

    return df1_monthly


print(create_monthly_df(w_df[4]).head(50))

import json

df_t0 = w_df[0]
cols_narrow = df_t0.columns.values.tolist()[1:]
print(cols_narrow)

print()

df_t1 = create_monthly_df(df_t0)  # dfw['0001']
print(len(df_t1))
# print(df_t1.head())

cols_wide = []
for i in range(0, len(df_t1)):
    row = df_t1.iloc[i]
    # print(row)
    # can't use date, because it has year built in, and weeks start on different numbers...
    month_id = 'month_' + str(i).zfill(2)
    # print(date)
    for c in cols_narrow:
        cols_wide.append(month_id + '__' + c)

print(cols_wide)
print(len(cols_wide))

# starts with a df with the weekly aggregates for weather params,
# and produces a long sequence of all the MONTHLY weather values, in order corresponding to cols_wide

print(w_df[0].columns.tolist()[1:])
print(w_df[0].shape)
print(create_monthly_df(w_df[0]).shape)

def create_weather_seq_for_monthly(dfw):
    seq = []
    cols = dfw.columns.tolist()
    for i in range(0,len(dfw)):
        for c in cols:
            seq.append(dfw.iloc[i][c])
    return seq

# sanity check
dfw = create_monthly_df(w_df[0])
print(dfw.head(10))

seqw = create_weather_seq_for_monthly(dfw)
print(json.dumps(seqw, indent=4))

import time

u_df = {}  # each entry will hold a df corresponding to a weather .csv file
dfw = {}  # each entry will hold the df corresponding to monthly aggregation of a weather .csv file
seqw = {}  # each entry will hold the "flattening" of the monthly aggregation df

for i in range(0, 6094):
    padded = str(i).zfill(4)
    # print(padded)
    u_df[padded] = pd.read_csv(weather_dir + wdtemplate.format(padded=padded))
    # Want to have a name for the index of my dataframe
    u_df[padded].rename(columns={'Unnamed: 0': 'date'},
                        inplace=True)

    dfw[padded] = create_monthly_df(u_df[padded])
    # print(dfw.head())

    seqw[i] = create_weather_seq_for_monthly(dfw[padded])
    # print(json.dumps(dictw, indent=4)

    # introducing a small occassional sleep because my python kernel kept complaining about
    # exceeding some I/O threshold
    if i % 30 == 0:
        time.sleep(0.05)

    if i > 9000 and i % 100 == 0:
        time.sleep(0.5)

    if i % 500 == 0:
        print('Completed processing for index: ', i)

# sanity check
# print(json.dumps(seqw, indent=4))


print(dfw['0000'].shape)
print(len(cols_wide))
print(len(df_yscyll))
print(len(seqw[0]))
print()

df_wide_weather_monthly = pd.DataFrame.from_dict(seqw, orient='index', columns=cols_wide)

print(df_wide_weather_monthly.shape)
print()
print(df_wide_weather_monthly.head())

sclls_file = 'state_county_lon_lat_soil.csv'

df_scsoil = pd.read_csv(archive_dir + sclls_file).drop(columns=['lon','lat'])
print(df_scsoil.shape)
# print(df_scsoil.head())

# will continue working with df_yscyll because updated DU PAGE county
#     (and might update other things in future versions...)

df_ysc_y_soil = pd.merge(df_yscyll, df_scsoil, on=['state_name','county_name'],how='left')

df_ysc_y_soil = df_ysc_y_soil.drop(columns=['lon','lat'])

print()
print(df_ysc_y_soil.shape)
print(df_ysc_y_soil.head())

df_ysc_y_soil_weather_monthly = pd.concat([df_ysc_y_soil, df_wide_weather_monthly], axis='columns')

print(df_ysc_y_soil_weather_monthly.shape)
# print(df_ysc_y_soil_weather_monthly.head(10))
print(df_ysc_y_soil_weather_monthly.loc[28:32,:])


ml_tables_dir = archive_dir + 'ML-TABLES/'

ml_file = 'ML-table-monthly.csv'

df_ysc_y_soil_weather_monthly.to_csv(ml_tables_dir + ml_file, index=False)

print('Wrote file ', ml_tables_dir + ml_file)