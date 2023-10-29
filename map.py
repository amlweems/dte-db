from glob import glob
from google.cloud import storage
import folium
import geopandas as gpd
import io
import json
import pandas as pd
import re

# init gcs client
gs = storage.Client()
bucket = gs.bucket('dte.lf.lc')

# Initialize a dictionary to store the aggregated data for each JOB_ID
outage_map = {}
crs = 'EPSG:4326'

# Import geojson from data/ directory
features = []
for fn in sorted(glob('data/**/jobs.json', recursive=True)):
    with open(fn, 'r') as f:
        data = json.load(f)
        features.extend(data.values())
geojson = {
    "type": "FeatureCollection",
    "crs": {
        "type": "name",
        "properties": {
            "name": crs,
        }
    },
    "features": features,
}

# load geojson into GeoDataFrame
outage_table = gpd.read_file(io.StringIO(json.dumps(geojson)), driver='GeoJSON')
outage_table.rename(columns={
    'JOB_ID':    'job_id',
    'OFF_DTTM':  'start',
    'ON_DTTM':   'end',
    'OFF_HOURS': 'length',
}, inplace=True)

# load Ann Arbor data from https://www.a2gov.org/services/data/Pages/default.aspx
landuse = gpd.read_file('data/landuse.geojson')
landuse = landuse.to_crs(outage_table.crs)
landuse['landuse_id'] = range(len(landuse))
landuse_outages = gpd.sjoin(landuse, outage_table,
    how='left', predicate='intersects').groupby('landuse_id')

landuse_events = []
for landuse_id, group in landuse_outages:
    group = group.sort_values(by='start')

    event_id = 0
    event_mapping = {}
    for index, row in group.iterrows():
        # if the current row started after the current event, make a new event
        if not event_mapping or row['start'] > event_mapping[event_id]['end']:
            event_id += 1
            event_mapping[event_id] = {
                'start': row['start'],
                'end': row['end'],
            }
        else:
            # otherwise, extend the event end time if needed
            event_mapping[event_id]['end'] = max(event_mapping[event_id]['end'], row['end'])
        outage_table.at[index, 'event_id'] = event_id
    for event_id, event in event_mapping.items():
        if pd.isna(event['start']) or pd.isna(event['end']):
            continue
        landuse_events.append({
            'landuse_id': int(landuse_id),
            'event_id':   int(event_id),
            'start':      int(event['start']),
            'end':        int(event['end']),
            'length':     int((event['end'] - event['start']) // (1000 * 60 * 60)),
        })
landuse_events = pd.DataFrame(landuse_events).groupby('landuse_id')

summary_table = landuse.copy()
# total
metric = landuse_events['length'].sum().reset_index()
metric.rename(columns={'length': 'total_length'}, inplace=True)
summary_table = summary_table.merge(metric, on='landuse_id')
# average
metric = landuse_events['length'].mean().reset_index()
metric.rename(columns={'length': 'avg_length'}, inplace=True)
summary_table = summary_table.merge(metric, on='landuse_id')
# median
metric = landuse_events['length'].median().reset_index()
metric.rename(columns={'length': 'median_length'}, inplace=True)
summary_table = summary_table.merge(metric, on='landuse_id')
# number of events
metric = landuse_events['event_id'].nunique().reset_index(name='outage_count')
metric.loc[metric['outage_count'] == 0, 'outage_count'] = None
summary_table = summary_table.merge(metric, on='landuse_id')

# plot summary data and export
m = summary_table.explore(
    column='total_length',
    name='Total Length',
    vmax=summary_table['total_length'].quantile(0.95),
    tiles='CartoDB positron',
    cmap='plasma',
    style_kwds={'stroke': False},
    missing_kwds={'color': '#00000000'}
)
m = summary_table.explore(m=m,
    column='avg_length',
    name='Average Length',
    vmax=summary_table['avg_length'].quantile(0.95),
    tiles='CartoDB positron',
    cmap='plasma',
    style_kwds={'stroke': False},
    missing_kwds={'color': '#00000000'}
)
m = summary_table.explore(m=m,
    column='median_length',
    name='Median Length',
    vmax=summary_table['median_length'].quantile(0.95),
    tiles='CartoDB positron',
    cmap='plasma',
    style_kwds={'stroke': False},
    missing_kwds={'color': '#00000000'}
)
m = summary_table.explore(m=m,
    column='outage_count',
    name='Number of Outages',
    vmax=summary_table['outage_count'].quantile(0.95),
    tiles='CartoDB positron',
    cmap='plasma',
    style_kwds={'stroke': False},
    missing_kwds={'color': '#00000000'}
)
folium.LayerControl().add_to(m)

# export html to gcs
with bucket.blob('outages/index.html').open('wb', content_type='text/html') as f:
    m.save(f)
