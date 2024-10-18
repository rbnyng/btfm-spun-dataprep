import asyncio
import aiohttp
import json
import logging
import os
import sys
from datetime import datetime
from pystac_client import Client
import shapely
from tqdm import tqdm

BATCH_SIZE = 32

band_assets = [
    "red", "blue", "green", "nir", "nir08", "nir09",
    "rededge1", "rededge2", "rededge3", "swir16", "swir22", "scl"
]

logging.basicConfig(level=logging.INFO)

async def download_asset(session, asset_href, output_path):
    # check if output file already exists, if so skip
    if os.path.exists(output_path):
        logging.info(f"Skipping download of {asset_href}")
        return

    tries = 0

    while tries < 3:
        tries += 1
        try:
            async with session.get(asset_href) as response:
                if response.status == 200:
                    content = await response.read()
                    with open(output_path, "wb") as f:
                        f.write(content)
                    break
                else:
                    logging.error(f"Failed to download {asset_href}: Status {response.status}. Trying again.")
        except asyncio.exceptions.TimeoutError:
            logging.error(f"Timeout downloading {asset_href}")
        except aiohttp.client_exceptions.ClientPayloadError:
            logging.error(f"Client payload error {asset_href}")

    if tries == 3:
        logging.error(f"Failed to download {asset_href}")

async def process_item(session, item, loc):
    item_dict = item.to_dict(include_self_link=False)
    with open(f"{loc}/{item.id}.json", "w") as f:
        json.dump(item_dict, f)

    tasks = []
    for asset in band_assets:
        asset_href = item.assets[asset].href
        output_path = f"{loc}/{asset}/{item.id}.tiff"
        task = download_asset(session, asset_href, output_path)
        tasks.append(task)

    await asyncio.gather(*tasks)

async def process_batch(batch, loc):
    async with aiohttp.ClientSession() as session:
        tasks = []
        for item in batch:
            if item.properties["s2:processing_baseline"] == "05.00":
                tasks.append(process_item(session, item, loc))
            else:
                logging.info(f"Skipping item {item.id} due to processing baseline")
        await asyncio.gather(*tasks)

async def main():
    if len(sys.argv) != 2:
        print("Usage: python download_tiles.py <geojson file>")
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

    loc = f"data/{filename}/"
    os.makedirs(loc, exist_ok=True)
    for asset in band_assets:
        os.makedirs(f"{loc}/{asset}", exist_ok=True)

    with tqdm(total=num_matched) as pbar:
        for batch in search.pages():
            logging.info(f"Processing batch of {len(batch)} items")
            await process_batch(batch, loc)
            pbar.update(len(batch))

if __name__ == "__main__":
    asyncio.run(main())