import requests
import pandas as pd
import time
import logging
import re
import os
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s",
    handlers=[logging.FileHandler("logs/api.log", encoding="utf-8"), logging.StreamHandler()])
logger = logging.getLogger(__name__)

API_KEY = os.getenv("LASTFM_API_KEY")
BASE_URL = "https://ws.audioscrobbler.com/2.0/"


def lastfm_get(method, **params):
    payload = {
        "method": method,
        "api_key": API_KEY,
        "format": "json",
        **params
    }
    response = requests.get(BASE_URL, params=payload, timeout=15)
    response.raise_for_status()
    data = response.json()
    if "error" in data:
        raise ValueError("Error", data['error'], ":", data.get('message'))
    return data


# метод 1 — album.getInfo
def get_album_info(artist, album):
    data = lastfm_get("album.getInfo", artist=artist, album=album)

    if "album" not in data:
        return {
            "artist": artist,
            "album_title": album,
            "listeners": None,
            "playcount": None,
            "tags": None,
            "wiki": None
        }

    album_data = data["album"]
    listeners = album_data.get("listeners")
    playcount = album_data.get("playcount")

    tags = None
    if "tags" in album_data and "tag" in album_data["tags"]:
        tags_list = album_data["tags"]["tag"]
        if isinstance(tags_list, list):
            tags = ", ".join([t["name"] for t in tags_list if "name" in t])

    wiki = None
    if "wiki" in album_data:
        raw = album_data["wiki"].get("summary", "")
        wiki = re.sub(r"<[^>]+>", "", raw).strip()[:500]

    return {
        "artist": artist,
        "album_title": album,
        "listeners": listeners,
        "playcount": playcount,
        "tags": tags,
        "wiki": wiki
    }


# метод 2 — artist.getInfo
def get_artist_info(artist):
    data = lastfm_get("artist.getInfo", artist=artist)
    artist_data = data.get("artist", {})
    bio_raw = artist_data.get("bio", {}).get("summary", "")
    bio = re.sub(r"<[^>]+>", "", bio_raw).strip()[:500]
    return bio


# метод 3 — track.getInfo
def get_track_info(artist, track):
    data = lastfm_get("track.getInfo", artist=artist, track=track)
    return data["track"]["listeners"]


# метод 4 — artist.getTopTracks
def get_artist_top_tracks(artist):
    data = lastfm_get("artist.getTopTracks", artist=artist)
    tracks = data["toptracks"]["track"]
    return ", ".join([t["name"] for t in tracks[:5]])



# метод 5 — artist.getTopAlbums
def get_artist_top_albums(artist):
    data = lastfm_get("artist.getTopAlbums", artist=artist)
    albums = data["topalbums"]["album"]
    return ", ".join([a["name"] for a in albums[:5]])



def collect_api_data(allmusic_df, sleep_sec=0.5):
    albums_df = allmusic_df[["artist", "album_title"]].drop_duplicates()

    results = []
    for i, row in albums_df.iterrows():
        try:
            artist = row["artist"]
            album = row["album_title"]

            album_info = get_album_info(artist, album)

            result = {
                "artist": artist,
                "album_title": album,
                "listeners": album_info["listeners"],
                "playcount": album_info["playcount"],
                "tags": album_info["tags"],
                "wiki": album_info["wiki"],
                "artist_bio": get_artist_info(artist),
                "top_tracks": get_artist_top_tracks(artist),
                "top_albums": get_artist_top_albums(artist)
            }

            results.append(result)
            logger.info(f"{i}: {artist} — {album}")

        except Exception as e:
            logger.error("Ошибка:", e)

        time.sleep(sleep_sec)

    return pd.DataFrame(results)


def run_api(allmusic_path="data/allmusic_final.csv"):
    os.makedirs("data", exist_ok=True)
    os.makedirs("logs", exist_ok=True)

    logger.info("Запуск сбора данных через API")

    allmusic_df = pd.read_csv(allmusic_path)
    api_df = collect_api_data(allmusic_df)
    api_df.to_csv("data/lastfm_api.csv", index=False)
    logger.info("Датасет сохранён")

    return api_df


if __name__ == "__main__":
    run_api()
