create or replace view Analytics.wr_points_qb_stats as (
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

  qb_rolling_stats as (
    select
      team_id,
      player,
      week,
      year,
      sum(passing_completions) 
        over (
          partition by player, year
          order by week
          rows between unbounded preceding and 1 preceding
        ) as rolling_passing_completions,
      sum(passing_atempts)
        over (
          partition by player, year
          order by week
          rows between unbounded preceding and 1 preceding
        ) as rolling_passing_attempts,
      avg(passing_touchdowns) 
        over (
          partition by player
          order by week
          rows between unbounded preceding and 1 preceding
        ) as avg_passing_tds,
      fantasy_points
    from Analytics.fct_fantasy_qb
  ),

  joined as (
  select
      wr.week,
      wr.year,
      wr.player,
      qb.player qb,
      wr.team_id,
      wr.fantasy_points as wr_fantasy_points,
      case when rolling_passing_attempts = 0 then 0
        else rolling_passing_completions / rolling_passing_attempts
        end as rolling_completion_pct,
      ifnull(qb.avg_passing_tds, 0) as rolling_avg_passing_tds,
      qb.fantasy_points as qb_fantasy_points
    from Analytics.fct_fantasy_wr wr
    inner join qb_rolling_stats qb
    on wr.team_id = qb.team_id and wr.week = qb.week and wr.year = qb.year
  )

  select
    *,
    case when rolling_completion_pct between 0 and 0.5 then "0-50%"
        when rolling_completion_pct between 0.5 and 0.6 then "51-60%"
        when rolling_completion_pct between 0.6 and 0.7 then "61-70%"
        when rolling_completion_pct between 0.7 and 0.8 then "71-80%"
        when rolling_completion_pct between 0.8 and 0.9 then "81-90%"
        when rolling_completion_pct between 0.9 and 1 then "91-100%"
    end as completion_pct_grade
    from joined
)
