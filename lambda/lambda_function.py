import json
import logging
import os
from decimal import Decimal, InvalidOperation

import psycopg2
from psycopg2.extras import execute_batch
import requests

logger = logging.getLogger()
logger.setLevel(logging.INFO)

API_BASE_URL = 'https://sedeaplicaciones.minetur.gob.es/ServiciosRESTCarburantes/PreciosCarburantes'


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def get_db_connection():
    return psycopg2.connect(
        host=os.environ['DB_HOST'],
        port=int(os.environ.get('DB_PORT', 5432)),
        dbname=os.environ['DB_NAME'],
        user=os.environ['DB_USER'],
        password=os.environ['DB_PASSWORD'],
    )


def parse_price(value):
    """Convert Spanish decimal string (comma separator) to Decimal, or None."""
    if not value or not str(value).strip():
        return None
    try:
        return Decimal(str(value).replace(',', '.'))
    except InvalidOperation:
        return None


def parse_coordinate(value):
    """Convert Spanish decimal string (comma separator) to float, or None."""
    if not value or not str(value).strip():
        return None
    try:
        return float(str(value).replace(',', '.'))
    except ValueError:
        return None


# ---------------------------------------------------------------------------
# Fetch functions
# ---------------------------------------------------------------------------

def fetch_communities():
    url = f'{API_BASE_URL}/Listados/ComunidadesAutonomas/'
    response = requests.get(url, timeout=30)
    response.raise_for_status()
    communities = []
    for item in response.text.split('|'):
        if item.strip():
            communities.append({'id': item[:2], 'name': item[2:]})
    logger.info('Fetched %d communities', len(communities))
    return communities


def fetch_provinces():
    url = f'{API_BASE_URL}/Listados/Provincias/'
    response = requests.get(url, timeout=30)
    response.raise_for_status()
    provinces = []
    for item in response.text.split('|'):
        if item.strip():
            provinces.append({'id': item[:2], 'name': item[2:]})
    logger.info('Fetched %d provinces', len(provinces))
    return provinces


def fetch_municipalities():
    url = f'{API_BASE_URL}/Listados/Municipios/'
    response = requests.get(url, timeout=30)
    response.raise_for_status()
    municipalities = []
    for item in response.text.split('|'):
        if item.strip():
            municipalities.append({'id': item[:4], 'name': item[4:]})
    logger.info('Fetched %d municipalities', len(municipalities))
    return municipalities


def fetch_gas_stations():
    url = f'{API_BASE_URL}/Listados/EstacionesAutomovil/'
    response = requests.get(url, timeout=60)
    response.raise_for_status()
    stations = response.json()['ListaEESSPrecio']
    logger.info('Fetched %d gas stations', len(stations))
    return stations


# ---------------------------------------------------------------------------
# DB functions
# ---------------------------------------------------------------------------

def create_tables(conn):
    with conn.cursor() as cursor:
        cursor.execute('''
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
        ''')
    conn.commit()
    logger.info('Tables and indexes created/verified')


def insert_communities(conn, data):
    sql = (
        'INSERT INTO communities (id, name) VALUES (%s, %s) '
        'ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name'
    )
    rows = [(c['id'], c['name']) for c in data]
    with conn.cursor() as cursor:
        execute_batch(cursor, sql, rows, page_size=500)
    conn.commit()
    logger.info('Upserted %d communities', len(rows))
    return len(rows)


def insert_provinces(conn, data):
    sql = (
        'INSERT INTO provinces (id, name) VALUES (%s, %s) '
        'ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name'
    )
    rows = [(p['id'], p['name']) for p in data]
    with conn.cursor() as cursor:
        execute_batch(cursor, sql, rows, page_size=500)
    conn.commit()
    logger.info('Upserted %d provinces', len(rows))
    return len(rows)


def insert_municipalities(conn, data):
    sql = (
        'INSERT INTO municipalities (id, name) VALUES (%s, %s) '
        'ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name'
    )
    rows = [(m['id'], m['name']) for m in data]
    with conn.cursor() as cursor:
        execute_batch(cursor, sql, rows, page_size=500)
    conn.commit()
    logger.info('Upserted %d municipalities', len(rows))
    return len(rows)


def insert_gas_stations(conn, stations):
    sql = '''
        INSERT INTO gas_stations (
            id, name, address, municipality_id, province_id,
            postal_code, latitude, longitude, geom,
            biodiesel_price, bioethanol_price, natural_gas_price,
            propane_price, butane_price,
            gasoline_95_price, gasoline_98_price,
            diesel_price, diesel_plus_price
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s,
            CASE WHEN %s IS NULL THEN NULL ELSE ST_GeomFromEWKT(%s) END,
            %s, %s, %s, %s, %s, %s, %s, %s, %s
        ) ON CONFLICT (id) DO UPDATE SET
            name = EXCLUDED.name,
            address = EXCLUDED.address,
            latitude = EXCLUDED.latitude,
            longitude = EXCLUDED.longitude,
            geom = EXCLUDED.geom,
            biodiesel_price = EXCLUDED.biodiesel_price,
            bioethanol_price = EXCLUDED.bioethanol_price,
            natural_gas_price = EXCLUDED.natural_gas_price,
            propane_price = EXCLUDED.propane_price,
            gasoline_95_price = EXCLUDED.gasoline_95_price,
            gasoline_98_price = EXCLUDED.gasoline_98_price,
            diesel_price = EXCLUDED.diesel_price,
            diesel_plus_price = EXCLUDED.diesel_plus_price,
            updated_at = CURRENT_TIMESTAMP
    '''

    rows = []
    skipped = 0
    for station in stations:
        station_id = station.get('IDEESS')
        try:
            lat = parse_coordinate(station.get('Latitud'))
            lon = parse_coordinate(station.get('Longitud_x0020__x0028_WGS84_x0029_'))

            geom = None
            if lat is not None and lon is not None:
                geom = f'SRID=4326;POINT({lon} {lat})'

            rows.append((
                station_id,
                station.get('Rótulo'),
                station.get('Dirección'),
                station.get('IDMunicipio'),
                station.get('IDProvincia'),
                station.get('C.P.'),
                lat,
                lon,
                geom,   # for the IS NULL check
                geom,   # for ST_GeomFromEWKT
                parse_price(station.get('Precio_x0020_Biodiesel')),
                parse_price(station.get('Precio_x0020_Bioetanol')),
                parse_price(station.get('Precio_x0020_Gas_x0020_Natural_x0020_Comprimido')),
                parse_price(station.get('Precio_x0020_Gases_x0020_licuados_x0020_del_x0020_petróleo')),
                None,   # butane_price — merged into propane_price (LPG) in the API
                parse_price(station.get('Precio_x0020_Gasolina_x0020_95_x0020_E5')),
                parse_price(station.get('Precio_x0020_Gasolina_x0020_98_x0020_E5')),
                parse_price(station.get('Precio_x0020_Gasoleo_x0020_A')),
                parse_price(station.get('Precio_x0020_Gasoleo_x0020_Premium')),
            ))
        except Exception:
            logger.warning('Skipping station %s due to unexpected error', station_id, exc_info=True)
            skipped += 1

    with conn.cursor() as cursor:
        execute_batch(cursor, sql, rows, page_size=500)
    conn.commit()

    inserted = len(rows)
    logger.info('Upserted %d gas stations, skipped %d', inserted, skipped)
    return inserted


# ---------------------------------------------------------------------------
# Handler
# ---------------------------------------------------------------------------

def lambda_handler(event, context):
    conn = get_db_connection()
    try:
        create_tables(conn)

        communities = fetch_communities()
        n_communities = insert_communities(conn, communities)

        provinces = fetch_provinces()
        n_provinces = insert_provinces(conn, provinces)

        municipalities = fetch_municipalities()
        n_municipalities = insert_municipalities(conn, municipalities)

        stations = fetch_gas_stations()
        n_stations = insert_gas_stations(conn, stations)

        summary = {
            'communities': n_communities,
            'provinces': n_provinces,
            'municipalities': n_municipalities,
            'gas_stations': n_stations,
        }
        logger.info('Pipeline complete: %s', summary)
        return {'statusCode': 200, 'body': json.dumps(summary)}

    except Exception as e:
        logger.exception('Pipeline failed: %s', e)
        return {'statusCode': 500, 'body': json.dumps({'error': str(e)})}

    finally:
        conn.close()
