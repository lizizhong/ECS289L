import pandas as pd

archive_dir = './Archive/'
yscyll_filename = 'year_state_county_yield_lon_lat.csv'

weather_dir = archive_dir + 'WEATHER/'
wdtemplate = r'weather-data-for-index__{padded}.csv'

df_yscyll = pd.read_csv(archive_dir + yscyll_filename)
print(df_yscyll.shape)
print()

w_df = {}
for i in range(0, len(df_yscyll)):
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
# produces dataframe with same shape, but the values are grouped by WEEK,
#   with a particular aggregation used for each column

def create_weekly_df(df):
    df1 = df.copy()
    # convert index to datetime format
    df1.index = pd.to_datetime(df['date'], format='%Y%m%d')
    # use 'M' for monthly, use 'W' for weekly
    df1_weekly = df1.resample('W').agg({'T2M_MAX': 'mean',
                                        'T2M_MIN': 'mean',
                                        'PRECTOTCORR': 'sum',
                                        'GWETROOT': 'mean',
                                        'EVPTRNS': 'mean',
                                        'ALLSKY_SFC_PAR_TOT': 'sum'})

    # convert index back to string format YYYYMM
    df1_weekly.index = df1_weekly.index.strftime('%Y%m%d')

    return df1_weekly


print(create_weekly_df(w_df[4]).head(50))

import json

df_t0 = w_df[0]
cols_narrow = df_t0.columns.values.tolist()[1:]
print(cols_narrow)

print()

df_t1 = create_weekly_df(df_t0)  # dfw['0001']
# print(df_t1.head())

cols_wide = []
for i in range(0, len(df_t1)):
    row = df_t1.iloc[i]
    # print(row)
    # can't use date, because it has year built in, and weeks start on different numbers...
    week_id = 'week_' + str(i).zfill(2)
    # print(date)
    for c in cols_narrow:
        cols_wide.append(week_id + '__' + c)

print(cols_wide)


# starts with a df with the weekly aggregates for weather params,
# and produces a long sequence of all the WEEKLY weather values, in order corresponding to cols_wide

print(w_df[0].columns.tolist()[1:])
print(w_df[0].shape)
print(create_weekly_df(w_df[0]).shape)

def create_weather_seq_for_weekly(dfw):
    seq = []
    for i in range(0,len(dfw)):
        for c in dfw.columns.tolist():
            seq.append(dfw.iloc[i][c])
    return seq

# sanity check
dfw = create_weekly_df(w_df[0])
print(dfw.head(10))

seqw = create_weather_seq_for_weekly(dfw)
print(json.dumps(seqw, indent=4))

import time

u_df = {}
dfw = {}
seqw = {}

for i in range(0, len(df_yscyll)):
    padded = str(i).zfill(4)
    # print(padded)
    u_df[padded] = pd.read_csv(weather_dir + wdtemplate.format(padded=padded))
    # Want to have a name for the index of my dataframe
    u_df[padded].rename(columns={'Unnamed: 0': 'date'},
                        inplace=True)

    dfw[padded] = create_weekly_df(u_df[padded])
    # print(dfw.head())

    seqw[i] = create_weather_seq_for_weekly(dfw[padded])
    # print(json.dumps(dictw, indent=4)

    # introducing a small occassional sleep because my python kernel kept complaining about
    # exceeding some I/O threshold
    # if i % 30 == 0:
    #     time.sleep(0.05)

    # if i > 4000 and i % 100 == 0:
    #     time.sleep(0.5)

    if i % 100 == 0:
        print('Completed processing of index ', i)

# sanity check
print(print(json.dumps(seqw, indent=4)))

print(dfw['0000'].shape)
print(len(cols_wide))
print(len(df_yscyll))
print(len(seqw[0]))
print()

df_wide_weather_weekly_prelim = pd.DataFrame.from_dict(seqw, orient='index', columns=cols_wide)

print(df_wide_weather_weekly_prelim.shape)
print()
print(df_wide_weather_weekly_prelim.head())

# print(cols_wide_weekly)
print(df_wide_weather_weekly_prelim.shape)
week_31_cols = ['week_31__T2M_MAX', 'week_31__T2M_MIN', 'week_31__PRECTOTCORR', 'week_31__GWETROOT', 'week_31__EVPTRNS', 'week_31__ALLSKY_SFC_PAR_TOT']

df_wide_weather_weekly = df_wide_weather_weekly_prelim.drop(columns=week_31_cols)

print()
print(df_wide_weather_weekly.shape)
print(df_wide_weather_weekly.head())

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

df_ysc_y_soil_weather_weekly = pd.concat([df_ysc_y_soil, df_wide_weather_weekly], axis='columns')

print(df_ysc_y_soil_weather_weekly.shape)
# print(df_ysc_y_soil_weather_weekly.head(10))
print(df_ysc_y_soil_weather_weekly.loc[28:32,:])

ml_tables_dir = archive_dir + 'ML-TABLES'

ml_file = 'ML-table-weekly.csv'

df_ysc_y_soil_weather_weekly.to_csv(ml_tables_dir + ml_file, index=False)

print('Wrote file ', ml_tables_dir + ml_file)

