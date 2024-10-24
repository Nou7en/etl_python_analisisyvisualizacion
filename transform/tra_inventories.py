import traceback
from util.db_connection import Db_Connection
import pandas as pd

def transformar_inventory():
    try:
        # Conexión a la base de datos OLTP
        type = 'mysql'
        host = 'localhost'
        port = '3306'
        user = 'root'
        pwd = 'tonychen777'
        db = 'oltp'
        
        con_oltp_db = Db_Connection(type, host, port, user, pwd, db)
        ses_oltp_db = con_oltp_db.start()

        # Extraemos datos de inventory y film
        sql_stmt_inventory = "SELECT inventory_id, film_id, store_id, last_update FROM inventory"
        inventory = pd.read_sql(sql_stmt_inventory, ses_oltp_db)

        sql_stmt_film = "SELECT film_id, rental_rate, replacement_cost FROM film"
        films = pd.read_sql(sql_stmt_film, ses_oltp_db)

        # Conexión a la base de datos SOR (Data Warehouse)
        db_sor = 'sor'
        con_sor_db = Db_Connection(type, host, port, user, pwd, db_sor)
        ses_sor_db = con_sor_db.start()

        # Extraemos datos de dim_date para hacer el join con last_update
        sql_stmt_dates = "SELECT id AS date_id, date_bk FROM dim_date"
        dates = pd.read_sql(sql_stmt_dates, ses_sor_db)

        # Generar date_bk en formato YYYYMMDD
        inventory['date_bk'] = inventory['last_update'].dt.strftime('%Y%m%d').astype(int)

        # Hacer merge con dim_date para obtener el date_id correspondiente
        inventory_merged = pd.merge(inventory, dates, how='left', on='date_bk')

        # Unir datos de inventory con los de film para añadir rental_rate y replacement_cost
        inventory_transformed = pd.merge(inventory_merged, films, on='film_id')

        # Renombrar columnas para coincidir con la tabla fact_inventory
        inventory_transformed = inventory_transformed.rename(columns={
            'inventory_id': 'id',
            'film_id': 'film_id',
            'store_id': 'store_id',
            'rental_rate': 'rental_price',
            'replacement_cost': 'rental_cost'
        })

        # Eliminar columnas innecesarias
        inventory_transformed = inventory_transformed.drop(columns=['last_update', 'date_bk'])

        return inventory_transformed

    except:
        traceback.print_exc()

    finally:
        pass