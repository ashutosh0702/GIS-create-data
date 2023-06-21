from pyproj import CRS, Transformer
from shapely.geometry import Polygon


def calc_geodesic_area(polygon_coords):
    
    # Define the CRS as WGS 84 (EPSG:4326)
    wgs84_crs = CRS('EPSG:4326')

    # Define the CRS for geodesic calculations in acres (US Survey Feet)
    #geodesic_crs = CRS(proj='aea', lat_1=polygon_coords[0][1], lat_2=polygon_coords[0][1], lat_0=polygon_coords[0][1], lon_0=polygon_coords[0][0], x_0=0, y_0=0, ellps='GRS80', units='us-ft')
    
    indian_crs = CRS('EPSG:32643')

    # Define a transformer to convert from WGS 84 to the geodesic CRS
    transformer = Transformer.from_crs(wgs84_crs, indian_crs, always_xy=True)

    # Transform the polygon coordinates to the Indian CRS
    indian_coords = [transformer.transform(lon, lat) for lon, lat in polygon_coords]


    # Create a Polygon object from the coordinates in the Indian CRS
    polygon = Polygon(indian_coords)

    # Calculate the area of the polygon in square meters
    area_m2 = polygon.area

    # Convert the area to acres
    area_acres = area_m2 * 0.000247105
    
    return area_acres
