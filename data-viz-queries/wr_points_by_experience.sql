create or replace view Analytics.wr_points_by_experience as (
  select
    week,
    year,
    experience,
    avg(fantasy_points) as avg_fantasy_points
  from Analytics.fct_fantasy_wr
  group by experience, week, year
)
