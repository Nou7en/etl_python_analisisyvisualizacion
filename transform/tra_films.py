import traceback
from util.db_connection import Db_Connection
import pandas as pd

def transformar_films():
    try:
        type = 'mysql'
        host = 'localhost'
        port = '3306'
        user = 'root'
        pwd = 'tonychen777'
        db = 'staging'

        con_sta_db = Db_Connection(type,host,port,user,pwd,db)
        ses_sta_db = con_sta_db.start()

        sql_stmt = "SELECT film_id, title, release_year, length, rental_rate, replacement_cost, last_update FROM ext_film"
        films = pd.read_sql(sql_stmt, ses_sta_db)

        # Transformamos la columna length para que se ajuste a las categorías
        films['length_category'] = films['length'].apply(lambda x: 
            '<1h' if x < 60 else 
            '<1.5h' if x < 90 else 
            '<2h' if x < 120 else '>2h'
        )

        films_transformed = films[['film_id', 'title', 'release_year', 'length_category', 'rental_rate', 'replacement_cost', 'last_update']]

        return films_transformed

    except:
        traceback.print_exc()

    finally:
        pass