from pathlib import Path
import glob
import gzip
import gzip
import json
import json
import logging
import re
import requests
import ssl
import time
import urllib3

# From https://stackoverflow.com/a/73519818
class CustomHttpAdapter (requests.adapters.HTTPAdapter):
    # "Transport adapter" that allows us to use custom ssl_context.

    def __init__(self, ssl_context=None, **kwargs):
        self.ssl_context = ssl_context
        super().__init__(**kwargs)

    def init_poolmanager(self, connections, maxsize, block=False):
        self.poolmanager = urllib3.poolmanager.PoolManager(
            num_pools=connections, maxsize=maxsize,
            block=block, ssl_context=self.ssl_context)

def get_legacy_session():
    ctx = ssl.create_default_context(ssl.Purpose.SERVER_AUTH)
    ctx.options |= 0x4  # OP_LEGACY_SERVER_CONNECT
    session = requests.session()
    session.mount('https://', CustomHttpAdapter(ctx))
    return session

def fetch_geojson(url, offset):
    """
    Fetch GeoJSON data from the given URL with the specified offset.

    Parameters:
        url (str): The URL to fetch the GeoJSON data.
        offset (int): The offset for pagination.

    Returns:
        dict or None: The GeoJSON data as a dictionary, or None if the request fails.
    """
    try:
        response = get_legacy_session().get(f'{url}&resultOffset={offset}')
        response.raise_for_status()
        return response.json()
    except requests.RequestException as e:
        logging.error(f'Request failed at offset {offset}: {e}')
        return None
    except json.JSONDecodeError as e:
        logging.error(f'Failed to parse JSON response at offset {offset}: {e}')
        return None

def fetch_geojson_with_retry(url, offset, max_retries=5):
    """
    Fetch GeoJSON data with retry logic.

    Parameters:
        url (str): The URL to fetch the GeoJSON data.
        offset (int): The offset for pagination.
        max_retries (int): Maximum number of retries (default is 5).

    Returns:
        dict or None: The GeoJSON data as a dictionary, or None if all retries fail.
    """
    for attempt in range(max_retries + 1):
        geojson = fetch_geojson(url, offset)
        if geojson:
            return geojson

        logging.warning(f'Retry {attempt + 1}/{max_retries} failed. Retrying in 1 second...')
        time.sleep(1)

    logging.error(f'All retries failed at offset {offset}.')
    return None

def merge(geojson, ts, root=Path('data/')):
    """ Merge features into directory structure
    """
    features = {}
    for feat in geojson:
        props = feat.get('properties', {})
        job_id = props.get('JOB_ID')
        if not job_id:
            continue
        off = props.get('OFF_DTTM')
        if not off:
            continue

        props['ON_DTTM'] = ts
        props['OFF_HOURS'] = (ts - off) // (1000 * 60 * 60)

        feat['id'] = job_id
        feat['properties'] = props

        key = job_id[2:8]
        if key in features:
            features[key].append(feat)
        else:
            features[key] = [feat]

    for key, feats in features.items():
        year, month, day = key[0:2], key[2:4], key[4:6]
        path = root / year / month / day / 'jobs.json'
        path.parent.mkdir(exist_ok=True, parents=True)
        data = {}
        if path.exists():
            with path.open('r') as f:
                data = json.load(f)
        data.update({feat.get('id'): feat for feat in feats})
        with path.open('w') as f:
            json.dump(data, f, indent=2, sort_keys=True)

def main():
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

    geojson_url = 'https://outagemap.serv.dteenergy.com/GISRest/services/OMP/OutageLocations/MapServer/2/query?WHERE=OBJECTID%3E0&outFields=*&f=geojson'
    ts = int(time.time()) * 1000
    features = []
    has_more_data = True
    offset = 0

    while has_more_data:
        logging.info(f'Fetching geojson, offset {offset}')
        geojson = fetch_geojson_with_retry(geojson_url, offset)
        if not geojson:
            break

        has_more_data = geojson.get('exceededTransferLimit', False)
        features.extend(geojson.get('features', []))

        offset += len(geojson.get('features', []))  # Update the offset dynamically

    print(f'Merging {len(features)} features')
    merge(features, ts)

if __name__ == '__main__':
    main()
