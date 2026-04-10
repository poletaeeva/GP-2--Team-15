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

if __name__ == "__main__":
    run_eda()
