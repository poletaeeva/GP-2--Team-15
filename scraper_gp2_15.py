

import requests
from bs4 import BeautifulSoup
import pandas as pd
import re
import time
import logging
from datetime import datetime, timedelta

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s",
    handlers=[logging.FileHandler("logs/scraper.log", encoding="utf-8"), logging.StreamHandler()])
logger = logging.getLogger(__name__)

HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/124.0.0.0 Safari/537.36"}
BASE_URL = "https://www.allmusic.com"

def clean_text(text):
    if text is None:
        return None
    return re.sub(r"\s+", " ", text).strip()

def normalize_url(href):
    if href.startswith("//"):
        return "https:" + href
    if href.startswith("/"):
        return BASE_URL + href
    return href

def get_soup(url):
    response = requests.get(url, headers=HEADERS, timeout=20)
    response.raise_for_status()
    return BeautifulSoup(response.text, "html.parser")

def generate_week_urls(start_date, weeks):
    start_dt = datetime.strptime(start_date, "%Y%m%d")
    result = []
    for i in range(weeks):
        current_date = start_dt - timedelta(days=7 * i)
        dt = current_date.strftime("%Y%m%d")
        url = f"{BASE_URL}/newreleases/all/{dt}"
        result.append((dt, url))
    return result

def parse_weekly_page(url, week):
    soup = get_soup(url)
    rows = []
    seen = set()

    for a_tag in soup.find_all("a", href=True):
        href = normalize_url(a_tag["href"])

        if "/album/" not in href or href in seen:
            continue
        seen.add(href)

        album_text = clean_text(a_tag.get_text(" ", strip=True))
        row = a_tag.parent.parent
        row_text = clean_text(row.get_text(" | ", strip=True))
        parts = [clean_text(x) for x in row_text.split(" | ") if clean_text(x)]

        if len(parts) < 4 or album_text not in parts:
            continue

        album_index = parts.index(album_text)
        artist_parts = [x for x in parts[:album_index] if x != "/"]
        artist = " / ".join(artist_parts) if artist_parts else None
        after = [x for x in parts[album_index + 1:] if x != "/"]

        if len(after) < 2:
            continue

        label = after[0]
        genre = after[1]
        rating = after[2] if len(after) > 2 else None

        rows.append({
            "week": week,
            "artist": artist,
            "album_title": album_text,
            "label": label,
            "genre": genre,
            "rating": rating,
            "album_url": href
        })

    df = pd.DataFrame(rows)
    logger.info(f" За {week} скачено {len(df)} строк")
    return df

def collect_weekly_releases(start_date, weeks, sleep_sec=1.1):
    week_urls = generate_week_urls(start_date, weeks)
    frames = []

    for dt, url in week_urls:
        try:
            df = parse_weekly_page(url, dt)
            frames.append(df)
        except Exception as e:
            logger.error(f"Ошибка для недели {dt}: {e}")
        time.sleep(sleep_sec)

    result = pd.concat(frames, ignore_index=True)
    logger.info(f"Скачено {len(result)} строк (за {weeks} недель)")
    return result


def run_scraper(start_date="20260410", n_weeks=20):
    import os
    os.makedirs("data", exist_ok=True)
    os.makedirs("logs", exist_ok=True)

    logger.info("Запуск парсера AllMusic")

    weekly_df = collect_weekly_releases(start_date, n_weeks)
    weekly_df.to_csv("data/allmusic_stage1.csv", index=False)
    logger.info(f"Получили {len(weekly_df)} строк")

    return weekly_df

if __name__ == "__main__":
    run_scraper()