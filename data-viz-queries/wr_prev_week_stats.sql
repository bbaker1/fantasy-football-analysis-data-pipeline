create or replace view Analytics.wr_prev_week_stats as (
  with prev_week as (
    select
      player,
      week,
      year,
      fantasy_points,
      lag(receiving_yards, 1) over (partition by player, year order by week) as prev_week_rec_yards,
      lag(receiving_yards, 2) over (partition by player, year order by week) as prev_2_week_rec_yards,
      lag(receiving_touchdowns, 1) over (partition by player, year order by week) as prev_week_rec_td,
      lag(receiving_touchdowns, 2) over (partition by player, year order by week) as prev_2_week_rec_td
    from Analytics.fct_fantasy_wr
  )

  select
    player,
    week,
    year,
    fantasy_points,
    case when prev_week_rec_yards between 0 and 25 then "0-25 yards"
        when prev_week_rec_yards between 25 and 50 then "26-50 yards"
        when prev_week_rec_yards between 50 and 75 then "51-75 yards"
        when prev_week_rec_yards between 75 and 100 then "75-100 yards"
        when prev_week_rec_yards > 100 then "Over 100 yards"
    end as prev_week_yards_grade,
    case when prev_2_week_rec_yards between 0 and 25 then "0-25 yards"
        when prev_2_week_rec_yards between 25 and 50 then "26-50 yards"
        when prev_2_week_rec_yards between 50 and 75 then "51-75 yards"
        when prev_2_week_rec_yards between 75 and 100 then "75-100 yards"
        when prev_2_week_rec_yards > 100 then "Over 100 yards"
    end as prev_2_week_yards_grade,
    case when prev_week_rec_td = 0 then "0 TDs"
        when prev_week_rec_td = 1 then "1 TD"
        when prev_week_rec_td > 2 then "2+ TDs"
    end as prev_week_td_grade,
    case when prev_2_week_rec_td = 0 then "0 TDs"
        when prev_2_week_rec_td = 1 then "1 TD"
        when prev_2_week_rec_td > 2 then "2+ TDs"
    end as prev_2_week_td_grade
  from prev_week
)
