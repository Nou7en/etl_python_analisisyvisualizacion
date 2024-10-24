import traceback
from util.db_connection import Db_Connection
import pandas as pd

def transformar_dates():
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

        # Extraemos los datos de ext_date (el CSV ya cargado en staging)
        sql_stmt = "SELECT date_id, date, month, year FROM ext_date"
        dates = pd.read_sql(sql_stmt, ses_sta_db)

        # Convertimos la columna 'date' a tipo datetime si no lo está
        dates['date'] = pd.to_datetime(dates['date']).dt.date

        # Transformamos las fechas para que coincidan con last_update de films
        dates_transformed = dates[['date', 'month', 'year']].rename(columns={
            'date': 'date_bk',         # Usamos el valor de 'date' para 'date_bk'
            'month': 'date_month',
            'year': 'date_year'
        })

        return dates_transformed

    except:
        traceback.print_exc()

    finally:
        pass