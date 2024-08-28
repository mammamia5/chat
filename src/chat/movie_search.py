import pandas as pd
import os
import requests
import json
import math
import time
# from tqdm import tqdm
# API_KEY
key = os.getenv('MOVIE_API_KEY')
# URL
def gen_url(movienm: str):
    base_url = 'http://kobis.or.kr/kobisopenapi/webservice/rest/movie/searchMovieList.json'
    url = f'{base_url}?key={key}&movieNm={movienm}'
    return url

# Load JSON
def req(movienm: str):
    url = gen_url(movienm)
    res = requests.get(url)
    data = res.json()
    return data
def read_data(movienm: str):
    df = req(movienm)
    result_df = df['movieListResult']['movieList']
    return result_df
