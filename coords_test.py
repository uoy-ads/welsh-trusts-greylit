from shapely.geometry import Point, Polygon

# Example of a more accurate boundary for Wales (hypothetical points)
# In practice, use a detailed shapefile or geojson data for accurate boundaries
wales_accurate_boundary = Polygon([
    (-5.3, 51.3), (-4.7, 51.4), (-3.0, 51.5), (-2.8, 52.0), (-3.5, 53.0),
    (-4.0, 53.2), (-5.0, 53.4), (-5.5, 53.0), (-5.3, 51.3)
])

# Coordinates to check
example_coordinates = [
    (-3.0811629, 52.7621706),  # Four Crosses barrow cemetery, (Bronze Age)
    (-3.082262, 52.7622682),   # Four Crosses barrow cemetery, ring ditch V
    (-3.0817343, 52.7619045)   # Four Crosses barrow cemetery, ring ditch VII
]

# Check if each point is within the accurate boundary of Wales
results = {str(coord): wales_accurate_boundary.contains(Point(coord)) for coord in example_coordinates}

print(results)
