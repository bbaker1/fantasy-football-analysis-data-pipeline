Description
================
I created a series of Python scripts to scrape Fantasy Football data every week (I also loaded 5 years of historical data). These scripts ultimately load fantasy, scores, schedules, and teams data to raw BigQuery tables where they are transformed by a dbt job. I also created a Looker dashboard using views that I created on top of my data model. This is a work-in-progress and I plan to build a model to predict player's fantasy points.

Airflow DAG Steps
================
1. Python script (BS4/Pandas) scrapes FootballGuys.com (Fantasy data) and ProFootballReference.com (Scores and Schedules)
2. CSV saved locally (the script overwrites the file each week so local memory is not affected)
3. For each file, upload to GCS using path for today's date (e.g. bucket/table/yyyy/mm/dd/data.csv)
4. Load each file from GCS into raw BigQuery table
5. Update the current week for next week's run
6. (Not in the DAG anymore because my dbt free-trial expired) Run dbt job to build data model and update Analytics tables

Tables
================
- fct_fantasy_wr
- fct_fantasy_qb
- fct_fantasy_te
- fct_fantasy_td
- fct_fantasy_rb
- fct_scores
- fct_schedules
- dim_teams
- dim_date

Tools
================
- Python
- Airflow
- Docker (Astronomer)
- GCS
- BigQuery
- dbt
- SQL
- Looker
