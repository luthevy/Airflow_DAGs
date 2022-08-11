import sqlite3
import pandas as pd


def db_to_csv():
    conn = sqlite3.connect('dags/weather.db',
                            isolation_level=None,
                            detect_types=sqlite3.PARSE_COLNAMES)
    tables_name = ['GEOLOC_DIMEN','WEATHER_DIMEN','SYS_DIMEN','WEATHER_FACT']
    for ele in tables_name:
        db_df = pd.read_sql_query("SELECT * FROM {}".format(ele), conn)
        db_df.to_csv('dags/data/{}.csv'.format(ele), index=False)


def read_sysdimen_csv():
    df = pd.read_csv('dags/data/SYS_DIMEN.csv')
    list_data = df.loc[:, ['y_id','yp','counry','unri','un']].values.tolist()
    return list_data

def read_geolocdimen_csv():
    df = pd.read_csv('dags/data/GEOLOC_DIMEN.csv')
    list_data = df.loc[:, ['coord_id','coord_lon','coord_la','imzon','id','nam','cod']].values.tolist()
    return list_data

def read_weatherdimen_csv():
    df = pd.read_csv('dags/data/WEATHER_DIMEN.csv')
    list_data = df.loc[:, ['wahr_id','main','dcripion','icon']].values.tolist()
    return list_data

def read_weatherfact_csv():
    df = pd.read_csv('dags/data/WEATHER_FACT.csv')
    list_data = df.loc[:, ['coord_id',
                           'wahr_id',
                           'y_id',
                           'ba',
                           'main_mp',
                           'main_fl_lik',
                           'main_prur',
                           'main_humidiy',
                           'main_mp_min',
                           'main_mp_max',
                           'main_a_lvl',
                           'main_grnd_lvl',
                           'viibiliy',
                           'wind_pd',
                           'wind_dg',
                           'wind_gu',
                           'cloud',
                           'rain_1h',
                           'rain_3h',
                           'snow_1h',
                           'snow_3h',
                           'd']].values.tolist()
    return list_data
