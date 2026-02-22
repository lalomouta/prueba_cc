from datetime import datetime, timedelta
import requests
import json
import psycopg2
from psycopg2.extras import Json
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

DAG_ID = 'fuel_prices_pipeline'

API_BASE_URL = 'https://sedeaplicaciones.minetur.gob.es/ServiciosRESTCarburantes/PreciosCarburantes'

def fetch_communities(**context):
    url = f'{API_BASE_URL}/Listados/ComunidadesAutonomas/'
    response = requests.get(url, timeout=30)
    response.raise_for_status()
    
    data = response.text.split('|')
    communities = []
    for item in data:
        if item.strip():
            communities.append({
                'id': item[:2],
                'name': item[2:]
            })
    
    context['task_instance'].xcom_push(key='communities', value=communities)
    return communities

def fetch_provinces(**context):
    url = f'{API_BASE_URL}/Listados/Provincias/'
    response = requests.get(url, timeout=30)
    response.raise_for_status()
    
    data = response.text.split('|')
    provinces = []
    for item in data:
        if item.strip():
            provinces.append({
                'id': item[:2],
                'name': item[2:]
            })
    
    context['task_instance'].xcom_push(key='provinces', value=provinces)
    return provinces

def fetch_municipies(**context):
    url = f'{API_BASE_URL}/Listados/Municipios/'
    response = requests.get(url, timeout=30)
    response.raise_for_status()
    
    data = response.text.split('|')
    municipalities = []
    for item in data:
        if item.strip():
            municipalities.append({
                'id': item[:4],
                'name': item[4:]
            })
    
    context['task_instance'].xcom_push(key='municipalities', value=municipalities)
    return municipalities

def fetch_gas_stations(**context):
    url = f'{API_BASE_URL}/Listados/EstacionesAutomovil/'
    response = requests.get(url, timeout=60)
    response.raise_for_status()
    
    stations = response.json()['ListaEESSPrecio']
    context['task_instance'].xcom_push(key='gas_stations', value=stations)
    return stations

def insert_communities(**context):
    postgres_hook = PostgresHook(postgres_conn_id='postgres_default')
    communities = context['task_instance'].xcom_pull(key='communities', task_ids='fetch_communities')
    
    conn = postgres_hook.get_conn()
    cursor = conn.cursor()
    
    cursor.executemany(
        'INSERT INTO communities (id, name) VALUES (%s, %s) '
        'ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name',
        [(c['id'], c['name']) for c in communities]
    )
    conn.commit()
    cursor.close()
    conn.close()
    
    return len(communities)

def insert_provinces(**context):
    postgres_hook = PostgresHook(postgres_conn_id='postgres_default')
    provinces = context['task_instance'].xcom_pull(key='provinces', task_ids='fetch_provinces')
    
    conn = postgres_hook.get_conn()
    cursor = conn.cursor()
    
    cursor.executemany(
        'INSERT INTO provinces (id, name) VALUES (%s, %s) '
        'ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name',
        [(p['id'], p['name']) for p in provinces]
    )
    conn.commit()
    cursor.close()
    conn.close()
    
    return len(provinces)

def insert_municipalities(**context):
    postgres_hook = PostgresHook(postgres_conn_id='postgres_default')
    municipalities = context['task_instance'].xcom_pull(key='municipalities', task_ids='fetch_municipies')
    
    conn = postgres_hook.get_conn()
    cursor = conn.cursor()
    
    cursor.executemany(
        'INSERT INTO municipalities (id, name) VALUES (%s, %s) '
        'ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name',
        [(m['id'], m['name']) for m in municipalities]
    )
    conn.commit()
    cursor.close()
    conn.close()
    
    return len(municipalities)

def insert_gas_stations(**context):
    postgres_hook = PostgresHook(postgres_conn_id='postgres_default')
    stations = context['task_instance'].xcom_pull(key='gas_stations', task_ids='fetch_gas_stations')
    
    conn = postgres_hook.get_conn()
    cursor = conn.cursor()
    
    inserted = 0
    for station in stations:
        try:
            lat = float(station.get('Latitud', 0)) if station.get('Latitud') else None
            lon = float(station.get('Longitud', 0)) if station.get('Longitud') else None
            
            geom = None
            if lat and lon:
                geom = f'SRID=4326;POINT({lon} {lat})'
            
            cursor.execute(
                '''INSERT INTO gas_stations (
                    id, name, address, municipality_id, province_id, 
                    postal_code, latitude, longitude, geom,
                    biodiesel_price, bioethanol_price, natural_gas_price, 
                    propane_price, butane_price, gasoline_95_price, 
                    gasoline_98_price, diesel_price, diesel_plus_price
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, 
                    ST_GeomFromText(%s, 4326) %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s
                ) ON CONFLICT (id) DO UPDATE SET
                    name = EXCLUDED.name, address = EXCLUDED.address,
                    latitude = EXCLUDED.latitude, longitude = EXCLUDED.longitude,
                    geom = EXCLUDED.geom, updated_at = CURRENT_TIMESTAMP
                ''',
                (
                    station.get('IDEESS'),
                    station.get('Nombre'),
                    station.get('Dirección'),
                    station.get('IDMunicipio'),
                    station.get('IDProvincia'),
                    station.get('Código Postal'),
                    lat, lon, geom, None,
                    station.get('Precio Biodiésel'),
                    station.get('Precio Bioetanol'),
                    station.get('Precio Gas Natural Comprimido'),
                    station.get('Precio Propano'),
                    station.get('Precio Butano'),
                    station.get('Precio Gasolina 95 Protección'),
                    station.get('Precio Gasolina 98'),
                    station.get('Precio Gasoleo A'),
                    station.get('Precio Gasoleo A+'),
                )
            )
            inserted += 1
        except Exception as e:
            continue
    
    conn.commit()
    cursor.close()
    conn.close()
    
    return inserted

with DAG(
    dag_id=DAG_ID,
    default_args=default_args,
    description='Pipeline to fetch fuel prices and store in PostgreSQL with PostGIS',
    schedule_interval='0 */6 * * *',
    catchup=False,
    max_active_runs=1,
    tags=['fuel', 'gas', 'postgis', 'spain'],
) as dag:

    create_tables = PostgresOperator(
        task_id='create_tables',
        postgres_conn_id='postgres_default',
        sql='''
            CREATE TABLE IF NOT EXISTS communities (
                id VARCHAR(2) PRIMARY KEY,
                name VARCHAR(100) NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );

            CREATE TABLE IF NOT EXISTS provinces (
                id VARCHAR(2) PRIMARY KEY,
                name VARCHAR(100) NOT NULL,
                community_id VARCHAR(2),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );

            CREATE TABLE IF NOT EXISTS municipalities (
                id VARCHAR(4) PRIMARY KEY,
                name VARCHAR(100) NOT NULL,
                province_id VARCHAR(2),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );

            CREATE TABLE IF NOT EXISTS gas_stations (
                id VARCHAR(10) PRIMARY KEY,
                name VARCHAR(200),
                address VARCHAR(300),
                municipality_id VARCHAR(4),
                province_id VARCHAR(2),
                postal_code VARCHAR(5),
                latitude DOUBLE PRECISION,
                longitude DOUBLE PRECISION,
                geom GEOMETRY(POINT, 4326),
                biodiesel_price DECIMAL(6,3),
                bioethanol_price DECIMAL(6,3),
                natural_gas_price DECIMAL(6,3),
                propane_price DECIMAL(6,3),
                butane_price DECIMAL(6,3),
                gasoline_95_price DECIMAL(6,3),
                gasoline_98_price DECIMAL(6,3),
                diesel_price DECIMAL(6,3),
                diesel_plus_price DECIMAL(6,3),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );

            CREATE INDEX IF NOT EXISTS idx_gas_stations_geom 
                ON gas_stations USING GIST (geom);
            CREATE INDEX IF NOT EXISTS idx_gas_stations_province 
                ON gas_stations (province_id);
            CREATE INDEX IF NOT EXISTS idx_gas_stations_prices 
                ON gas_stations (diesel_price, gasoline_95_price);
        '''
    )

    fetch_communities_task = PythonOperator(
        task_id='fetch_communities',
        python_callable=fetch_communities,
    )

    fetch_provinces_task = PythonOperator(
        task_id='fetch_provinces',
        python_callable=fetch_provinces,
    )

    fetch_municipies_task = PythonOperator(
        task_id='fetch_municipies',
        python_callable=fetch_municipies,
    )

    fetch_gas_stations_task = PythonOperator(
        task_id='fetch_gas_stations',
        python_callable=fetch_gas_stations,
    )

    insert_communities_task = PythonOperator(
        task_id='insert_communities',
        python_callable=insert_communities,
    )

    insert_provinces_task = PythonOperator(
        task_id='insert_provinces',
        python_callable=insert_provinces,
    )

    insert_municipies_task = PythonOperator(
        task_id='insert_municipalities',
        python_callable=insert_municipalities,
    )

    insert_gas_stations_task = PythonOperator(
        task_id='insert_gas_stations',
        python_callable=insert_gas_stations,
    )

    create_tables >> [
        fetch_communities_task, 
        fetch_provinces_task, 
        fetch_municipies_task,
        fetch_gas_stations_task
    ]

    [fetch_communities_task, fetch_provinces_task, fetch_municipies_task] >> insert_communities_task
    fetch_communities_task >> insert_communities_task
    fetch_provinces_task >> insert_provinces_task
    fetch_municipies_task >> insert_municipies_task
    fetch_gas_stations_task >> insert_gas_stations_task
