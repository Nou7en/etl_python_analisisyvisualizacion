import traceback
from util.db_connection import Db_Connection
import pandas as pd

def cargar_stores():
    try:
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
            raise Exception("Error tratando de conectarse a la base de datos ")
    
        sql_stmt = "SELECT store_id, name, city, country\
            FROM tra_store"

        stores_tra = pd.read_sql (sql_stmt, ses_sta_db)
        
    
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
            raise Exception("Error tratando de conectarse a la base de datos ")
        
        
        dim_sto_dict = {
            "store_bk": [],
            "name": [],
            "city": [],
            "country": []
        }

        if not stores_tra.empty:
            for bk, nam, cit, cou in zip(stores_tra['store_id'], stores_tra['name'], stores_tra['city'], stores_tra['country']):
                dim_sto_dict['store_bk'].append(bk)
                dim_sto_dict['name'].append(nam)
                dim_sto_dict['city'].append(cit)
                dim_sto_dict['country'].append(cou)

        if dim_sto_dict['store_bk']:
            df_dim_store = pd.DataFrame(dim_sto_dict)
            df_dim_store.to_sql('dim_store', ses_sor_db, if_exists='append', index=False)

    except:
        traceback.print_exc()

    finally:
        pass