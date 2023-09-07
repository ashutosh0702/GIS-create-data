import json
import boto3
import geopandas as gpd
from shapely.geometry import Polygon
import os
import zipfile
import pandas as pd
import geodesic_utils as gs
import base64

# Constants and Configuration
bucket_name = "boundary-plot"
bucket_agro = "agro-climatic-zones"
file_key = "type-texture-zone.zip"
EPSG_4326 = 'EPSG:4326'

ph_scale = {
    'ALLUVIAL_SOIL': '6.5 - 8.4',
    'Desert': '7.6 - 8.4',
    'BLACK_SOIL': '6.5 - 8.4',
    'RED_SOIL': '5.2 - 7.5',
    'MIXED RED & BLA': '6.5 - 7.5'
}

# Set up the S3 client
s3 = boto3.client('s3')

def lambda_handler(event, context):

    print(event,type(event))

    decoded_body = base64.b64decode(event["body"]).decode('utf-8')
    ev = json.loads(decoded_body)

    try:
        farmer_name = ev["name"]
        farmer_id = ev["id"]
        coords = ev['coords']
    except KeyError as e:
        return {
            "statusCode": 400,
            "body": json.dumps(f"Missing key: {e}")
        }

    area = gs.calc_geodesic_area(coords)
    polygon = Polygon(coords)
    polygon_gdf = gpd.GeoDataFrame(index=[0], crs=EPSG_4326, geometry=[polygon])
    print(polygon_gdf)
    # Download and extract shapefile
    s3.download_file(bucket_agro, file_key, os.path.join("/tmp", file_key))
    os.chdir("/tmp")
    with zipfile.ZipFile(file_key, 'r') as zip_ref:
        zip_ref.extractall()
    params_df = gpd.read_file(os.path.join("/tmp", "type-texture-zone.shp"), allow_override=True)
    params_df.to_crs(epsg=4326, inplace=True)

    # Perform spatial join
    res = gpd.sjoin(polygon_gdf, params_df, how='left', predicate='within')
    print(f"res : {res}")
    if pd.isna(res['NAME_1'][0]):
        response = {
            "statusCode": 200,
            "body": json.dumps({
                "message": "No data for the given farm, farm cannot be saved"
            })
        }
    else:
        final = {
            'state_name': res['NAME_1'][0],
            'district_name': res['NAME_2'][0],
            'soil_type': res['SOIL_TYPE'][0],
            'ag_zone': res['layer'][0],
            'soil_texture': res['layer_2'][0],
            "area": area
        }

        final['soil_ph'] = ph_scale.get(final['soil_type'], 'Unknown')

        soil_type_mapping = {
            "BLACK_SOIL": "Black",
            "ALLUVIAL_SOIL": "Alluvial",
            "RED_SOIL": "Red",
            "MIXED RED & BLA": "Mixed red & black"
        }
        final['soil_type'] = soil_type_mapping.get(final['soil_type'], final['soil_type'])

        response = {
            "statusCode": 200,
            "headers": {
                "Content-Type": "application/json"
            },
            "body": json.dumps(final)
        }

        # Store the GeoJSON object in an S3 bucket
        polygon_feature = {
            "type": "Feature",
            "geometry": {
                "type": "Polygon",
                "coordinates": [coords]
            },
            "properties": {
                "area": area,
                "District": res['NAME_2'][0]
            }
        }
        geojson_bytes = bytes(json.dumps(polygon_feature).encode('UTF-8'))
        object_key = f"{farmer_id}_{farmer_name}.geojson"
        s3.put_object(Body=geojson_bytes, Bucket=bucket_name, Key=object_key)

    return response
