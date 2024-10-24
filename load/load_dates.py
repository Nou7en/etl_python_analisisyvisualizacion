import traceback
from util.db_connection import Db_Connection
import pandas as pd

def cargar_dates():
    try:
        # Conexión a la base de datos de staging
        type = 'mysql'
        host = 'localhost'
        port = '3306'
        user = 'root'
        pwd = 'tonychen777'
        db = 'staging'

        con_sta_db = Db_Connection(type, host, port, user, pwd, db)
        ses_sta_db = con_sta_db.start()

        # Extraemos los datos transformados de tra_dates
        sql_stmt = "SELECT * FROM tra_dates"
        dates_tra = pd.read_sql(sql_stmt, ses_sta_db)

        # Conexión a la base de datos de sor (data warehouse)
        type = 'mysql'
        host = 'localhost'
        port = '3306'
        user = 'root'
        pwd = 'tonychen777'
        db = 'sor'

        con_sor_db = Db_Connection(type, host, port, user, pwd, db)
        ses_sor_db = con_sor_db.start()

        # Cargamos los datos en la tabla dim_date
        dates_tra.to_sql('dim_date', ses_sor_db, if_exists='append', index=False)

    except:
        traceback.print_exc()

    finally:
        pass