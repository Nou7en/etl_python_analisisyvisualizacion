import traceback
from util.db_connection import Db_Connection
import pandas as pd

def extraer_stores():
    try:
        type = 'mysql'
        host = 'localhost'
        port = '3306'
        user = 'root'
        pwd = 'tonychen777'
        db = 'oltp'
        
        con_db = Db_Connection(type,host,port,user,pwd,db)
        ses_db = con_db.start()
        if ses_db == -1:
            raise Exception("El tipo de base de datos dado no es válido")
        elif ses_db == -2:
            raise Exception("Error tratando de conectarse a la base de datos ")
    
        stores = pd.read_sql ('SELECT * FROM store', ses_db)
        return stores

    except:
        traceback.print_exc()

    finally:
        pass