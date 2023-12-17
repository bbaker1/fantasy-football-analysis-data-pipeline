import numpy as np
import pandas as pd
from datetime import datetime

def get_schedule_data(year):
    url = f'https://www.pro-football-reference.com/years/{year}/games.htm'
    schedule_df = pd.read_html(url, header=0)[0]
#    schedule_df = schedule_df[schedule_df.Week != 'Week']
#    schedule_df = schedule_df[schedule_df.Week.apply(lambda x: x.isnumeric())]
    schedule_df = schedule_df[pd.to_numeric(schedule_df['Week'], errors='coerce').notnull()]
    schedule_df.to_csv(f'/usr/local/airflow/dags/scripts/get_data/data/schedules/schedule.csv')
    
    return
