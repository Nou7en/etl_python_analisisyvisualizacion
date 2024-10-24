import traceback
from util.db_connection import Db_Connection
import pandas as pd

def cargar_films():
    try:
        # Conexión a la base de datos de staging
        type = 'mysql'
        host = 'localhost'
        port = '3306'
        user = 'root'
        pwd = 'tonychen777'
        db = 'staging'
        
        con_sta_db = Db_Connection(type,host,port,user,pwd,db)
        ses_sta_db = con_sta_db.start()
        if ses_sta_db == -1:
            raise Exception("El tipo de base de datos dado no es válido")
        elif ses_sta_db == -2:
            raise Exception("Error tratando de conectarse a la base de datos staging")
    
        # Extraer datos de la tabla tra_films
        sql_stmt = "SELECT film_id, title, release_year, length_category, rental_rate, replacement_cost, last_update FROM tra_films"
        films_tra = pd.read_sql(sql_stmt, ses_sta_db)
        
        # Conexión a la base de datos de sor
        type = 'mysql'
        host = 'localhost'
        port = '3306'
        user = 'root'
        pwd = 'tonychen777'
        db = 'sor'
        
        con_sor_db = Db_Connection(type,host,port,user,pwd,db)
        ses_sor_db = con_sor_db.start()
        if ses_sor_db == -1:
            raise Exception("El tipo de base de datos dado no es válido")
        elif ses_sor_db == -2:
            raise Exception("Error tratando de conectarse a la base de datos sor")
        
        # Creación del diccionario para almacenar los datos transformados
        dim_film_dict = {
            "film_bk": [],
            "title": [],
            "release_year": [],
            "length_category": [],
            "rental_rate": [],
            "replacement_cost": [],
            "last_update": []  # Agregamos last_update
        }

        # Verificamos si la tabla tra_films no está vacía
        if not films_tra.empty:
            for fid, tit, year, length_cat, rate, cost, last_upd in zip(films_tra['film_id'], films_tra['title'], films_tra['release_year'], films_tra['length_category'], films_tra['rental_rate'], films_tra['replacement_cost'], films_tra['last_update']):
                dim_film_dict['film_bk'].append(fid)
                dim_film_dict['title'].append(tit)
                dim_film_dict['release_year'].append(year)
                dim_film_dict['length_category'].append(length_cat)
                dim_film_dict['rental_rate'].append(rate)
                dim_film_dict['replacement_cost'].append(cost)
                dim_film_dict['last_update'].append(last_upd)  # Insertamos last_update

        # Cargamos los datos transformados a la tabla dim_film de sor
        if dim_film_dict['film_bk']:
            df_dim_film = pd.DataFrame(dim_film_dict)
            df_dim_film.to_sql('dim_film', ses_sor_db, if_exists='append', index=False)

    except:
        traceback.print_exc()

    finally:
        pass