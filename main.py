import os
from dotenv import load_dotenv

load_dotenv()

from scraper_gp2_15 import run_scraper
from api import run_api


def main():
    os.makedirs("data", exist_ok=True)
    os.makedirs("logs", exist_ok=True)
    os.makedirs("plots", exist_ok=True)

    run_scraper(start_date="20260410", n_weeks=20)
    run_api(allmusic_path="data/allmusic_final.csv")

if __name__ == "__main__":
    main()
