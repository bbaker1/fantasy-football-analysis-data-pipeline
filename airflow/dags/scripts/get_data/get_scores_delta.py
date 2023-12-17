import requests
import pandas as pd
from bs4 import BeautifulSoup
from datetime import datetime

def get_scores_data(year):
    week = int(open('/usr/local/airflow/dags/scripts/get_data/data/current_week.txt', 'r').read())
    url = f'https://www.pro-football-reference.com/years/{year}/week_{week}.htm'
    soup = BeautifulSoup(requests.get(url).text, 'html.parser')
    raw_scores = []
    for div in soup.find_all('table', attrs={'class': 'teams'}):
        raw_scores.append(div)

    scores = []
    for score in raw_scores:
        row = {}
        date = score.find_all('tr')[0].text
        row['game_date'] = datetime.strptime(date, '%b %d, %Y').date()
        for ele in score.find_all('tr', attrs={'class': 'winner'}):
            tr = ele.find_all('td')
            row['winner_team'] = tr[0].text
            row['winner_score'] = tr[1].text
        for ele in score.find_all('tr', attrs={'class': 'loser'}):
            tr = ele.find_all('td')
            row['loser_team'] = tr[0].text
            row['loser_score'] = tr[1].text
        row['week'] = week
        row['year'] = year
        scores.append(row)
    
    scores_df = pd.DataFrame(scores)
    scores_df.to_csv('/usr/local/airflow/dags/scripts/get_data/data/scores/scores.csv')
    return
