create or replace view Analytics.wr_points_team_and_opponent_grade as (
  with record_by_week as (
    select
      id as team_id,
      name as team_name,
      week,
      year,
      sum(case when id = winner_team_id then 1 else 0 end) 
        over (
          partition by name, year
          order by week
          rows between unbounded preceding and 1 preceding
        ) as num_wins,
      sum(case when id = loser_team_id then 1 else 0 end)
        over (
          partition by name, year
          order by week
          rows between unbounded preceding and 1 preceding
        ) as num_losses
    from Analytics.dim_teams teams
    inner join Analytics.fct_scores scores
    on teams.id = scores.winner_team_id or teams.id = scores.loser_team_id
    group by name, winner_team_id, loser_team_id, id, week, year
  ),

  team_win_pct as (
    select
      team_id,
      team_name,
      week,
      year,
      num_wins,
      num_losses,
      case when num_wins = 0 then 0
          when num_losses = 0 then 1
          else num_wins / (num_wins + num_losses)
          end as win_pct
    from record_by_week
  ),

  wr_vs_opponent as (
    select
      wr.week,
      wr.year,
      player,
      wr.team_id as player_team_id,
      team1.team_name as player_team_name,
      team1.win_pct as player_team_win_pct,
      fantasy_points,
      team2.team_id as opponent_team_id,
      team2.team_name as opponent_team_name,
      team2.win_pct as opponent_team_win_pct
    from Analytics.fct_fantasy_wr wr
    inner join Analytics.fct_schedules sched
    on (wr.team_id = sched.team1_id or wr.team_id = team2_id)
      and wr.week = sched.week
    inner join team_win_pct as team1
    on wr.team_id = team1.team_id
      and wr.week = team1.week
    inner join team_win_pct team2
    on (sched.team1_id = team2.team_id or sched.team2_id = team2.team_id)
      and sched.week = team2.week 
      and team2.team_id != wr.team_id
      and extract(year from sched.game_date) = wr.year
  ),

  team_grades as (
    select
      *,
      case when opponent_team_win_pct between 0 and 0.1 then "0-10%"
          when opponent_team_win_pct between 0.1 and 0.2 then "11-20%"
          when opponent_team_win_pct between 0.2 and 0.3 then "21-30%"
          when opponent_team_win_pct between 0.3 and 0.4 then "31-40%"
          when opponent_team_win_pct between 0.4 and 0.5 then "41-50%"
          when opponent_team_win_pct between 0.5 and 0.6 then "51-60%"
          when opponent_team_win_pct between 0.6 and 0.7 then "61-70%"
          when opponent_team_win_pct between 0.7 and 0.8 then "71-80%"
          when opponent_team_win_pct between 0.8 and 0.9 then "81-90%"
          when opponent_team_win_pct between 0.9 and 1 then "91-100%"
          else "First game"
      end as opponent_grade,
      case when player_team_win_pct between 0 and 0.1 then "0-10%"
          when player_team_win_pct between 0.1 and 0.2 then "11-20%"
          when player_team_win_pct between 0.2 and 0.3 then "21-30%"
          when player_team_win_pct between 0.3 and 0.4 then "31-40%"
          when player_team_win_pct between 0.4 and 0.5 then "41-50%"
          when player_team_win_pct between 0.5 and 0.6 then "51-60%"
          when player_team_win_pct between 0.6 and 0.7 then "61-70%"
          when player_team_win_pct between 0.7 and 0.8 then "71-80%"
          when player_team_win_pct between 0.8 and 0.9 then "81-90%"
          when player_team_win_pct between 0.9 and 1 then "91-100%"
          else "First game"
      end as player_team_grade
    from wr_vs_opponent
    )

    select
      op.week,
      op.year,
      op.opponent_grade as team_grade,
      case when op.week = 1 then 0 else op.fantasy_points end as vs_opponent,
      case when op.week = 1 then 0 else play.fantasy_points end as player_team
    from team_grades as op
    inner join team_grades as play
    on op.opponent_grade = play.player_team_grade
)

