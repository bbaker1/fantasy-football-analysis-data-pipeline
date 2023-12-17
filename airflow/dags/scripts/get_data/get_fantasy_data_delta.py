import pandas as pd
from datetime import datetime

def get_fantasy_data(position, year):
    week = int(open('/usr/local/airflow/dags/scripts/get_data/data/current_week.txt', 'r').read())
    url = f'https://www.footballguys.com/playerhistoricalstats?pos={position}&yr={year}&startwk={week}&stopwk={week}&profile=p'
    fantasy_df = pd.read_html(url, header=0)[0]
    fantasy_df['week'] = week
    fantasy_df['year'] = year
    fantasy_df['position'] = position
    fantasy_df.to_csv(f'/usr/local/airflow/dags/scripts/get_data/data/fantasy/fantasy_{position}.csv')
    return
