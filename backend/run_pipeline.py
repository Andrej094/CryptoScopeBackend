import time

from filter1_scrape_symbols import get_symbols
from filter2_check_existing_data import get_existing_status
from filter3_download_missing import update_data


def run_pipeline():
    start_time = time.perf_counter()

    print("\n=== Filter 1: Scraping and cleaning top 1000 crypto symbols ===")
    get_symbols(limit=1000)

    print("\n=== Filter 2: Checking last available date in the DB ===")
    get_existing_status()

    print("\n=== Filter 3: Downloading missing OHLCV data into DB ===")
    update_data(workers=50)

    end_time = time.perf_counter()
    elapsed = end_time - start_time

    print(f"\nPIPELINE COMPLETE ✔  (Total time: {elapsed:.2f} seconds)")


if __name__ == "__main__":
    run_pipeline()
