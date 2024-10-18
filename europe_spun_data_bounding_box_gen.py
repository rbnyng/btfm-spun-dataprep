import csv
import json
from typing import List, Tuple

def read_csv(file_path: str) -> List[Tuple[float, float]]:
    coordinates = []
    with open(file_path, 'r') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            try:
                lon = float(row['longitude'])
                lat = float(row['latitude'])
                coordinates.append((lon, lat))
            except ValueError:
                continue
    return coordinates

def find_bounding_box(coordinates: List[Tuple[float, float]]) -> Tuple[float, float, float, float]:
    lons, lats = zip(*coordinates)
    return min(lons), min(lats), max(lons), max(lats)

def create_geojson(bbox: Tuple[float, float, float, float]) -> dict:
    min_lon, min_lat, max_lon, max_lat = bbox
    return {
        "type": "FeatureCollection",
        "features": [
            {
                "type": "Feature",
                "properties": {
                    "name": "Europe Bounding Box"
                },
                "geometry": {
                    "type": "Polygon",
                    "coordinates": [
                        [
                            [min_lon, min_lat],
                            [max_lon, min_lat],
                            [max_lon, max_lat],
                            [min_lon, max_lat],
                            [min_lon, min_lat]
                        ]
                    ]
                }
            }
        ]
    }

def main():
    file_path = 'data\spun_data\AMF_richness_europe.csv'
    coordinates = read_csv(file_path)
    bbox = find_bounding_box(coordinates)
    geojson = create_geojson(bbox)
    
    with open('europe_spun.json', 'w') as f:
        json.dump(geojson, f, indent=2)

    print("GeoJSON file 'europe_spun.json' has been created.")

if __name__ == "__main__":
    main()
