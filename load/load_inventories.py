import traceback
from util.db_connection import Db_Connection
import pandas as pd

def cargar_inventory():
    try:
        type = 'mysql'
        host = 'localhost'
        port = '3306'
        user = 'root'
        pwd = 'tonychen777'
        db_staging = 'staging'
        db_sor = 'sor'
        
        # Conexión a la base de datos staging
        con_sta_db = Db_Connection(type, host, port, user, pwd, db_staging)
        ses_sta_db = con_sta_db.start()

        # Conexión a la base de datos SOR
        con_sor_db = Db_Connection(type, host, port, user, pwd, db_sor)
        ses_sor_db = con_sor_db.start()

        # Extraer datos de tra_inventory desde staging
        sql_stmt = """
            SELECT ti.id, ti.film_id, ti.store_id, ti.date_id, ti.rental_price, ti.rental_cost
            FROM staging.tra_inventory ti
        """
        inventory_tra = pd.read_sql(sql_stmt, ses_sta_db)

        # Cargar los datos en la tabla fact_inventory en la base de datos SOR
        inventory_tra.to_sql('fact_inventory', ses_sor_db, if_exists='append', index=False)

    except:
        traceback.print_exc()

    finally:
        pass