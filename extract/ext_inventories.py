import traceback
from util.db_connection import Db_Connection
import pandas as pd

def extraer_inventories():
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

        # Extraemos datos de la tabla inventory
        sql_stmt_inventory = "SELECT inventory_id, film_id, store_id, last_update FROM inventory"
        inventories = pd.read_sql(sql_stmt_inventory, ses_oltp_db)
        
        return inventories
        

    except:
        traceback.print_exc()

    finally:
        pass