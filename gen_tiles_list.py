import asyncio
import logging
import sys
from datetime import datetime
from pystac_client import Client
import shapely
from tqdm import tqdm

logging.basicConfig(level=logging.INFO)

async def process_item(item, output_file):
    # Write the metadata .json filename to the output file
    output_file.write(f"{item.id}.json\n")

async def process_batch(batch, output_file):
    for item in batch:
        if item.properties["s2:processing_baseline"] == "05.00":
            await process_item(item, output_file)
        else:
            logging.info(f"Skipping item {item.id} due to processing baseline")

async def main():
    if len(sys.argv) != 2:
        print("Usage: python script_name.py <geojson file>")
        sys.exit(1)

    geojson_file = sys.argv[1]
    filename = geojson_file.split("/")[-1].split(".")[0]
    api_url = "https://earth-search.aws.element84.com/v1"
    collection = "sentinel-2-l2a"
    client = Client.open(api_url)

    with open(geojson_file) as f:
        geojson = shapely.from_geojson(f.read())

    start_time = datetime(2020, 1, 1)
    end_time = datetime(2021, 1, 1)

    search = client.search(
        collections=[collection],
        intersects=geojson,
        datetime=(start_time, end_time),
        query={"s2:processing_baseline": "05.00"}
    )

    num_matched = search.matched()
    logging.info(f"Matched {num_matched} items")

    output_file_path = f"{filename}_metadata_json_files.txt"
    
    with open(output_file_path, "w") as output_file, tqdm(total=num_matched) as pbar:
        for batch in search.pages():
            logging.info(f"Processing batch of {len(batch)} items")
            await process_batch(batch, output_file)
            pbar.update(len(batch))

    logging.info(f"Metadata JSON file names have been written to {output_file_path}")

if __name__ == "__main__":
    asyncio.run(main())