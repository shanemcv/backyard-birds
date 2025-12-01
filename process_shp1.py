import geopandas as gpd
import pandas as pd
import numpy as np
from shapely.geometry import Point
import time

start = time.time()

# load biome shapefile
biomes = gpd.read_file("NA_CEC_Eco_Level3/NA_CEC_Eco_Level3.shp")
biomes = biomes.to_crs(epsg=4326) # recasts to lat long if not already

# lat long grid creation
min_lat, max_lat = 20, 75     # latitude range
min_lon, max_lon = -170, -50  # longitude range
lat_vals = np.round(np.arange(min_lat, max_lat, 0.1), 2)
lon_vals = np.round(np.arange(min_lon, max_lon, 0.1), 2)
grid = pd.DataFrame([(lat, lon) for lat in lat_vals for lon in lon_vals], columns=['lat','lon'])

print("Completed making grid")

# Convert to GeoDataFrame
grid_gdf = gpd.GeoDataFrame(grid, geometry=[Point(xy) for xy in zip(grid['lon'], grid['lat'])], crs="EPSG:4326")

print("Finished converting GeoDataFrame")

# Spatial join for biomes
biomes_small = biomes[['NA_L3CODE','NA_L3NAME','geometry']]
grid_with_biomes = gpd.sjoin(grid_gdf, biomes_small, how='left', predicate='within')
grid_with_biomes = grid_with_biomes[['lat','lon','NA_L3CODE','NA_L3NAME']]


grid_with_biomes.to_csv("biome_lookup01.csv", index=False)
print("CSV saved as biome_lookup01.csv")

end = time.time()

print(f"Completed in {round(end - start, 3)} seconds. Estimated time for larger file: {100 * round(end - start, 3)} seconds or {(100 * round(end - start, 3)) / 60} minutes.")