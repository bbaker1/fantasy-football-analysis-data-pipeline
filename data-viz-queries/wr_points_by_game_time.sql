create or replace view Analytics.wr_points_by_game_time as (
  select distinct 
    scores.week,
    scores.year,
    extract(month from scores.game_date) as game_month,
    concat(left(game_time, 1), right(game_time, 2)) as game_hour,
    avg(fantasy_points) as avg_fantasy_points
  from Analytics.dim_teams teams
  inner join Analytics.fct_scores scores
  on teams.id = scores.winner_team_id or teams.id = scores.loser_team_id
  inner join Analytics.fct_schedules sched
  on (teams.id = sched.team1_id or teams.id = team2_id) and sched.game_date = scores.game_date
  inner join Analytics.fct_fantasy_wr as wr
  on teams.id = wr.team_id and wr.week = scores.week and wr.year = scores.year
  group by scores.week, scores.year, extract(month from scores.game_date), concat(left(game_time, 1), right(game_time, 2))
)
