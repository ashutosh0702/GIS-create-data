import json
import boto3
import geopandas as gpd
from shapely.geometry import Polygon
import os
import zipfile
import pandas as pd
import geodesic_utils as gs
import base64

# Set up the S3 client
s3 = boto3.client('s3')
bucket_name = "boundary-plot"

bucket_agro = "agro-climatic-zones"  # assigning s3 bucket name 
file_key = "type-texture-zone.zip"  

ph_scale = {
        'ALLUVIAL_SOIL': '6.5 - 8.4',
        'Desert' : '7.6 - 8.4',
        'BLACK_SOIL' : '6.5 - 8.4',
        'RED_SOIL' : '5.2 - 7.5',
        'MIXED RED & BLA' : '6.5 - 7.5' 
    }


def lambda_handler(event, context):
    
    
    print(event)
    decoded_body = base64.b64decode(event["body"]).decode('utf-8')
    
    print(decoded_body, type(decoded_body))
    
    ev = json.loads(decoded_body)
    
    print(ev,type(ev))
    
    #ev = json.loads(event["body"])
    
    try:
        farmer_name = ev["name"]
        farmer_id = ev["id"]
        coords = ev['coords']
    except:
        return {
            "statuscode" : 400,
            "body" : json.dumps("Missing body parameters")
        }
    
    
    area = gs.calc_geodesic_area(coords)
    
    # Define the GeoJSON feature as a dictionary
    polygon_feature = {
        "type": "Feature",
        "geometry": {
            "type": "Polygon",
            "coordinates": [coords]
        },
        "properties": {
            "area": area,
        }
    }
    
    
    
    polygon = Polygon(coords)

    # create a geopandas GeoDataFrame with the polygon feature
    polygon_gdf = gpd.GeoDataFrame(index=[0], crs='EPSG:4326', geometry=[polygon])

    # creating a client using default session s3
    s3.download_file(bucket_agro, file_key, os.path.join("/tmp",file_key))
    
    os.chdir("/tmp")
    with zipfile.ZipFile("type-texture-zone.zip", 'r') as zip_ref:
        zip_ref.extractall()
        
    params_df = gpd.read_file(os.path.join("/tmp","type-texture-zone.shp"),allow_override=True)
    
    params_df.to_crs(epsg=4326, inplace=True) # georeferencing the file in epsg:4326 format
    
    res = gpd.sjoin(polygon_gdf, params_df, how = 'left', predicate='within') # performing left spatial join by extracting the common intersecting area
    
    print(res['NAME_1'][0])
    
    if pd.isna(res['NAME_1'][0]):
        
        response = {
            "statusCode":200,
            "body" : json.dumps({
                "message" : "No data for the given farm , farm caanot be saved"
            })
        }
    else :
    
        final = {
            'state_name' : res['NAME_1'][0],
            'district_name' : res['NAME_2'][0],
            'soil_type' : res['SOIL_TYPE'][0],
            'ag_zone' : res['layer'][0],
            'soil_texture' : res['layer_2'][0],
            "area" : area
        }
        
        final['soil_ph'] = ph_scale[final['soil_type']]
    
        if final['soil_type'] == "BLACK_SOIL":
            final['soil_type'] = "Black"
        elif final['soil_type'] == "ALLUVIAL_SOIL":
            final['soil_type'] = "Alluvial"
        elif final['soil_type'] == "RED_SOIL":
            final['soil_type'] = "Red"
        elif final['soil_type'] == "MIXED RED & BLA":
            final['soil_type'] = "Mixed red & black"
        
        
    
        response = {
            "statusCode": 200,
            "headers": {
                "Content-Type": "application/json"
            },
            "body": json.dumps(final)
        }
        
        polygon_feature["properties"]["District"] = res['NAME_2'][0]
        
        # Convert the GeoJSON object to a string
        geojson_bytes = bytes(json.dumps(polygon_feature).encode('UTF-8'))
        object_key = f"{farmer_id}_{farmer_name}.geojson"
    
        # Store the GeoJSON object in an S3 bucket
    
        s3.put_object(Body=geojson_bytes, Bucket=bucket_name, Key=object_key)
    
        print("Checking for CI/CD")
    return response
    
    
    