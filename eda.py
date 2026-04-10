import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import re
import logging
from collections import Counter

logger = logging.getLogger(__name__)


def run_eda():
    df1 = pd.read_csv('data/allmusic_final.csv')
    df1 = df1.drop(columns=['album_url', 'image_url', 'text_description'])
    df1 = df1[df1['rating'].isna() | df1['rating'].str.isnumeric()]
    df1['rating'] = df1['rating'].astype(float)

    df1['styles'] = df1['styles'].str.replace(r'\s*(Listen on|Set Your|Log In).*$', '', regex=True)
    df1['styles'] = df1['styles'].str.strip()
    df1['styles'] = df1['styles'].str.rstrip(',').str.strip()

    
    df1 = df1.drop_duplicates(subset=['artist', 'album_title'], keep='first')

    df1['week_date'] = pd.to_datetime(df1['week_date'].astype(str), format='%Y%m%d')
    df1['release_date'] = pd.to_datetime(df1['release_date'], format='%B %d, %Y', errors='coerce')
    df1['duration'] = df1['duration'].str.split(':').apply(lambda x: round(int(x[0]) + int(x[1]) / 60) if type(x) == list else None)

if __name__ == "__main__":
    run_eda()
