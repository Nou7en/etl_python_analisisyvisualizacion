
import traceback
from util.db_connection import Db_Connection
import pandas as pd

def transformar_stores():
    try:
        type = 'mysql'
        host = 'localhost'
        port = '3306'
        user = 'root'
        pwd = 'tonychen777'
        db = 'staging'
        
        con_db = Db_Connection(type,host,port,user,pwd,db)
        ses_db = con_db.start()
        if ses_db == -1:
            raise Exception("El tipo de base de datos dado no es válido")
        elif ses_db == -2:
            raise Exception("Error tratando de conectarse a la base de datos ")
    
        sql_stmt = "SELECT s.store_id, concat('SAKILA Store ', s.store_id) AS name, \
        ifnull(ci.city, concat('City ', s.store_id)) AS city, \
        ifnull(co.country, concat('Country ', s.store_id)) AS country \
        FROM ext_store AS s \
        LEFT JOIN ext_address AS a ON (s.address_id = a.address_id) \
        LEFT JOIN ext_city AS ci ON (a.city_id = ci.city_id) \
        LEFT JOIN ext_country AS co ON (ci.country_id = co.country_id)"

        stores_tra = pd.read_sql (sql_stmt, ses_db)
        return stores_tra

    except:
        traceback.print_exc()

    finally:
        pass
