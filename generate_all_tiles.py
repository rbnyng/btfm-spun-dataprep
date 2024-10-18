import polars as pl
import sys
import logging
from tqdm import tqdm
from tqdm.contrib.logging import logging_redirect_tqdm
import tables as tb
import blosc2 as b2
import os
import numpy as np
import rasterio
from tqdm.contrib.concurrent import process_map
from functools import partial

logging.basicConfig(level=logging.INFO)

SAMPLE_RATE = 10 # 1% compression (1 in 10 pixels in each dimension)

BAND_ASSETS = [
    "red", "blue", "green", "nir", "nir08", "nir09",
    "rededge1", "rededge2", "rededge3", "swir16", "swir22", "scl"
]

ASSET_RATIOS = {
    "red": 1,
    "blue": 1,
    "green": 1,
    "nir": 1,
    "nir08": 2,
    "nir09": 6,
    "rededge1": 2,
    "rededge2": 2,
    "rededge3": 2,
    "swir16": 2,
    "swir22": 2
}

def process_tile(tile_name_df, directory):
    tile_name, tile_df = tile_name_df

    logging.info(f"Processing tile {tile_name}")

    # check if the tile has already been processed, if it has skip
    if os.path.exists(f"{directory}/processed/{tile_name}/bands.npy"):
        logging.info(f"Tile {tile_name} already processed, skipping")
        return

    os.makedirs(f"{directory}/processed/{tile_name}", exist_ok=True)

    tile_df.write_parquet(f"{directory}/processed/{tile_name}/metadata.parquet")
    tile_df.write_ndjson(f"{directory}/processed/{tile_name}/metadata.json")

    num_rows = tile_df.height

    masks = np.zeros((num_rows, 10980 // SAMPLE_RATE, 10980 // SAMPLE_RATE), dtype=np.uint8)
    day_of_years = np.zeros(num_rows, dtype=np.uint16)
    bands = np.zeros((num_rows, 10980 // SAMPLE_RATE, 10980 // SAMPLE_RATE, 11), dtype=np.uint16)

    for i, row in tqdm(enumerate(tile_df.iter_rows(named=True)), total=num_rows):
        id = row["id"]
        logging.info(f"Processing {id}")
        date = row["datetime"]
        day_of_year = date.timetuple().tm_yday
        day_of_years[i] = day_of_year
        path = f"{directory}/scl/{id}.tiff"
        with rasterio.open(path) as ds:
            band = ds.read(1)
            data = band.astype(np.uint8)
            # we need to mask (set false) 0, 1, 2, 3, 8 and 9
            mask = np.isin(data, [0, 1, 2, 3, 8, 9])
            mask = mask.astype(np.uint8)
            # invert
            mask = 1 - mask
            # upscale the mask to 10m (it is 20m)
            mask = np.repeat(np.repeat(mask, 2, axis=0), 2, axis=1)

        for j, asset in enumerate(BAND_ASSETS):
            if asset == "scl":
                continue

            path = f"{directory}/{asset}/{id}.tiff"
            with rasterio.open(path) as ds:
                nodataval = int(ds.nodata)
                band = ds.read(1)
                reapeated_band = np.repeat(np.repeat(band, ASSET_RATIOS[asset], axis=0), ASSET_RATIOS[asset], axis=1)
                nodata = reapeated_band == nodataval
                mask[nodata] = 0
                bands[i, :, :, j] = reapeated_band[::SAMPLE_RATE, ::SAMPLE_RATE]

        masks[i] = mask[::SAMPLE_RATE, ::SAMPLE_RATE]

        logging.info(f"Finished processing {id}")

    # write out the bands and masks to disk
    logging.info("Writing bands and masks to disk")

    np.save(f"{directory}/processed/{tile_name}/bands.npy", bands)
    np.save(f"{directory}/processed/{tile_name}/masks.npy", masks)
    np.save(f"{directory}/processed/{tile_name}/doys.npy", day_of_years)

    logging.info(f"Finished processing tile {tile_name}")

# first argument is the directory in data to pull all the tiles from
if len(sys.argv) != 2:
    print("Usage: python generate_all_tiles.py <directory>")
    sys.exit(1)

with logging_redirect_tqdm():
    directory = f"data/{sys.argv[1]}"

    q = pl.read_parquet(f"{directory}/tiles.parquet")

    df = q.select([pl.col("properties").struct.field("grid:code"), pl.col("id"), pl.col("properties").struct["datetime"].str.to_datetime(format="%Y-%m-%dT%H:%M:%S%.fZ"), pl.col("assets"), pl.col("properties")])

    df = df.sort(["grid:code", "datetime"])

    total_rows = len(df)

    logging.info(f"Processing {total_rows} rows")

    tile_names = df.select("grid:code").unique()["grid:code"].to_list()

    logging.info(f"Processing {len(tile_names)} tiles")

    os.makedirs(f"{directory}/processed", exist_ok=True)

    tile_names_dfs = [(tile_name, df.filter(pl.col("grid:code") == tile_name)) for tile_name in tile_names]

    # parallel version
    #process_map(partial(process_tile, directory=directory), tile_names_dfs, max_workers=80)

    # sequential version
    for tile_name_df in tile_names_dfs:
        process_tile(tile_name_df, directory)