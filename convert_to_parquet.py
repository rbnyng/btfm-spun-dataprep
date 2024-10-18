import sys
# take the first argument and convert each json to parquet
import polars as pl
import os

def main():
    if len(sys.argv) != 2:
        print("Usage: python convert_to_parquet.py <directory>")
        sys.exit(1)

    directory = f"data/{sys.argv[1]}"
    
    q = pl.scan_ndjson(f"{directory}/*.json")

    q.collect().write_parquet(f"{directory}/tiles.parquet")

if __name__ == "__main__":
    main()