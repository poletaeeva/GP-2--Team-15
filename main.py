import os
from dotenv import load_dotenv

load_dotenv()

from scraper import run_scraper



def main():
    os.makedirs("data", exist_ok=True)
    os.makedirs("logs", exist_ok=True)
    os.makedirs("plots", exist_ok=True)

    run_scraper(start_date="20260410", n_weeks=20)


if __name__ == "__main__":
    main()
