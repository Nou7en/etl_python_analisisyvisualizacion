from util.db_connection import Db_Connection
from extract.ext_countries import extraer_countries
from extract.ext_stores import extraer_stores
from extract.per_staging import persistir_staging
from extract.ext_addresses import extraer_addresses
from extract.ext_cities import extraer_cities
from transform.tra_stores import transformar_stores
from load.load_stores import cargar_stores
from extract.ext_dates import extraer_dates
from extract.ext_films import extraer_films
from transform.tra_films import transformar_films
from extract.ext_inventories import extraer_inventories
from load.load_films import cargar_films
from transform.tra_dates import transformar_dates
from load.load_dates import cargar_dates
from transform.tra_inventories import transformar_inventory
from load.load_inventories import cargar_inventory

import traceback
import pandas as pd

try:
    # Conexión a la base de datos
    con_db = Db_Connection('mysql', 'localhost', '3306', 'root', 'tonychen777', 'oltp')
    ses_db = con_db.start()
    if ses_db == -1:
        raise Exception("El tipo de base de datos dado no es válido")
    elif ses_db == -2:
        raise Exception("Error tratando de conectarse a la base de datos ")

    databases = pd.read_sql('SELECT COUNT(*) FROM oltp.customer', ses_db)
    print(databases)

    # Extrayendo y transformando datos de inventario
    print("Extrayendo datos de inventory desde la base de datos OLTP")
    inventories = extraer_inventories()  # Aquí extraes los datos
    print("Guardando en Staging los datos extraídos de inventory")
    persistir_staging(inventories, 'ext_inventory')

    print("Transformando datos de inventory en el staging")
    inventory_tra = transformar_inventory()  # Asegúrate de que esta función retorna un DataFrame

    # Verificando y renombrando columnas
    if 'id' in inventory_tra.columns:
        inventory_tra.rename(columns={'id': 'inventory_id'}, inplace=True)

    # Verificar si la columna 'date_id' está presente, si no, crearla con valores nulos
    if 'date_id' not in inventory_tra.columns:
        print("La columna 'date_id' no existe en el DataFrame, creando una columna vacía...")
        inventory_tra['date_id'] = None

    # Rellenar valores nulos en 'date_id' con un valor por defecto (por ejemplo, 1)
    inventory_tra['date_id'].fillna(1, inplace=True)

    # Conexión a la base de datos de destino (SOR)
    ses_sor_db = con_db.start()
    print("Cargando datos de inventory en SOR")
    inventory_tra.to_sql('fact_inventory', ses_sor_db, if_exists='append', index=False)

    # Otros procesos que tengas
    print("Extrayendo datos de countries desde un csv")
    countries = extraer_countries()
    print("Guardando en staging datos de countries")
    persistir_staging(countries, 'ext_country')

    # Procesos para cargar stores
    print("Extrayendo datos de stores desde una db")
    stores = extraer_stores()
    print("Guardando en staging datos de stores")
    persistir_staging(stores, 'ext_store')
    print("Transformando datos de store en el staging")
    tra_stores = transformar_stores()
    print("Persistiendo en stagging datos transformados de stores")
    persistir_staging(tra_stores, 'tra_store')
    print("Cargando datos de stores en sor")
    cargar_stores()

    # Procesos para films
    print("Extrayendo datos de films desde la base de datos OLTP")
    films = extraer_films()
    print("Guardando en Staging los datos extraídos de films")
    persistir_staging(films, 'ext_film')
    print("Transformando datos de films en Staging")
    films_transformed = transformar_films()
    print("Guardando los datos transformados en tra_films")
    persistir_staging(films_transformed, 'tra_films')
    cargar_films()

except Exception as e:
    traceback.print_exc()

finally:
    if ses_db:
        con_db.stop()
