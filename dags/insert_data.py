from dataflow_processing import read_geolocdimen_csv
from dataflow_processing import read_sysdimen_csv
from dataflow_processing import read_weatherdimen_csv
from dataflow_processing import read_weatherfact_csv
from airflow.providers.postgres.operators.postgres import PostgresOperator

def insert_geoloc_table():
    row = read_geolocdimen_csv()
    return PostgresOperator(
            task_id='insert_GEOLOC_table',
            postgres_conn_id='postgres_localhost',
            sql=('INSERT into GEOLOC_DIMEN VALUES(%i,%f,%f,%s,%s,%s,%s) ON CONFLICT (coord_id) DO NOTHING;'%(row[0][0],row[0][1],row[0][2],"\'"+(row[0][3])+"\'",str(row[0][4]),"\'"+(row[0][5])+"\'",str(row[0][6])),\
            'INSERT into GEOLOC_DIMEN VALUES(%i,%f,%f,%s,%s,%s,%s) ON CONFLICT (coord_id) DO NOTHING;'%(row[1][0],row[1][1],row[1][2],"\'"+(row[1][3])+"\'",str(row[1][4]),"\'"+(row[1][5])+"\'",str(row[1][6])),\
            'INSERT into GEOLOC_DIMEN VALUES(%i,%f,%f,%s,%s,%s,%s) ON CONFLICT (coord_id) DO NOTHING;'%(row[2][0],row[2][1],row[2][2],"\'"+(row[2][3])+"\'",str(row[2][4]),"\'"+(row[2][5])+"\'",str(row[2][6])))
        )

def insert_sys_table():
    row = read_sysdimen_csv()
    return PostgresOperator(
            task_id='insert_SYS_table',
            postgres_conn_id='postgres_localhost',
            sql=('INSERT into SYS_DIMEN VALUES(%s,%s,%s,%s,%s) ON CONFLICT (y_id) DO NOTHING;'%(str(row[0][0]),str(row[0][1]),"\'"+row[0][2]+"\'","\'"+row[0][3]+"\'","\'"+row[0][4]+"\'"),\
            'INSERT into SYS_DIMEN VALUES(%s,%s,%s,%s,%s) ON CONFLICT (y_id) DO NOTHING;'%(str(row[1][0]),str(row[1][1]),"\'"+row[1][2]+"\'","\'"+row[1][3]+"\'","\'"+row[1][4]+"\'"),\
            'INSERT into SYS_DIMEN VALUES(%s,%s,%s,%s,%s) ON CONFLICT (y_id) DO NOTHING;'%(str(row[2][0]),str(row[2][1]),"\'"+row[2][2]+"\'","\'"+row[2][3]+"\'","\'"+row[2][4]+"\'"))
        )

def insert_weatherdimen_table():
    row = read_weatherdimen_csv()
    return PostgresOperator(
            task_id='insert_WEATHER_table',
            postgres_conn_id='postgres_localhost',
            sql=('INSERT into WEATHER_DIMEN VALUES(%s,%s,%s,%s) ON CONFLICT (wahr_id) DO NOTHING;'%(str(row[0][0]),"\'"+row[0][1]+"\'","\'"+row[0][2]+"\'","\'"+row[0][3]+"\'"),\
            'INSERT into WEATHER_DIMEN VALUES(%s,%s,%s,%s) ON CONFLICT (wahr_id) DO NOTHING;'%(str(row[1][0]),"\'"+row[1][1]+"\'","\'"+row[1][2]+"\'","\'"+row[1][3]+"\'"),\
            'INSERT into WEATHER_DIMEN VALUES(%s,%s,%s,%s) ON CONFLICT (wahr_id) DO NOTHING;'%(str(row[2][0]),"\'"+row[2][1]+"\'","\'"+row[2][2]+"\'","\'"+row[2][3]+"\'"))
        )

def insert_weatherfact_table():
    row = read_weatherfact_csv()
    return PostgresOperator(
            task_id='insert_WEATHERFACT_table',
            postgres_conn_id='postgres_localhost',
            sql=(
                'INSERT into WEATHER_FACT VALUES(%i,%s,%s,%s,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%s) ON CONFLICT (coord_id,wahr_id,y_id,d) DO NOTHING;'\
                    %(row[0][0],str(row[0][1]),str(row[0][2]),"\'"+row[0][3]+"\'",row[0][4],row[0][5],row[0][6],row[0][7],\
                      row[0][8], row[0][9], row[0][10], row[0][11], row[0][12], row[0][13], row[0][14], row[0][15], row[0][16],\
                      row[0][17], row[0][18], row[0][19], row[0][20],"\'"+row[0][21]+"\'"),
                'INSERT into WEATHER_FACT VALUES(%i,%s,%s,%s,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%s) ON CONFLICT (coord_id,wahr_id,y_id,d) DO NOTHING;'\
                    %(row[1][0],str(row[1][1]),str(row[1][2]),"\'"+row[1][3]+"\'",row[1][4],row[1][5],row[1][6],row[1][7],\
                      row[1][8], row[1][9], row[1][10], row[1][11], row[1][12], row[1][13], row[1][14], row[1][15], row[1][16],\
                      row[1][17], row[1][18], row[1][19], row[1][20],"\'"+row[1][21]+"\'"),
                'INSERT into WEATHER_FACT VALUES(%i,%s,%s,%s,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%s) ON CONFLICT (coord_id,wahr_id,y_id,d) DO NOTHING;'\
                    %(row[2][0],str(row[2][1]),str(row[2][2]),"\'"+row[2][3]+"\'",row[2][4],row[2][5],row[2][6],row[2][7],\
                      row[2][8], row[2][9], row[2][10], row[2][11], row[2][12], row[2][13], row[2][14], row[2][15], row[2][16],\
                      row[2][17], row[2][18], row[2][19], row[2][20],"\'"+row[2][21]+"\'"))
        )
