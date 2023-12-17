def update_week():
    prev_week = int(open('/usr/local/airflow/dags/scripts/get_data/data/current_week.txt', 'r').read())
    with open('/usr/local/airflow/dags/scripts/get_data/data/current_week.txt', 'w') as outfile:
        outfile.write(str(prev_week+1))
