with filter_match as ( -- 43 match 17 series
	select 
		d.series_id, match_id,  max_match, radiant_team_id, dire_team_id, radiant_win, start_time
	FROM (
		select series_id, 
			case when series_type = 1 then 3 when series_type = 2 then 5 end as max_match
		from league_match_stage
		group by series_id, series_type
		having count(match_id) > 1
	) d
	LEFT JOIN league_match_stage l using (series_id)
)
, explode_match as ( -- 86 match
	SELECT 
	match_id, series_id, radiant_team_id as team_id, radiant_win as is_win
	FROM filter_match
	union all
	SELECT match_id, series_id, dire_team_id as team_id, not radiant_win as is_win
	FROM filter_match
) 
, 
match_win_tally as ( -- 34 series
	SELECT series_id, team_id,  0 as draw_count, win_count, max_match - win_count as lose_count
	FROM ( 
		select -- 34
			series_id, team_id, sum(is_win::int) as win_count, count(is_win) as max_match
		from explode_match
		group by series_id, team_id ) l 
) 

INSERT INTO series_fact(
	series_id, team_id, date_key, draw_count, win_count, lost_count)
select l.series_id, team_id, d.date_key, draw_count, win_count, lose_count
	from match_win_tally l
	left join
		(select distinct series_id, l.series_date, d.date, d.date_key 
		 from 
		 	(select distinct series_id, date(start_time) as series_date from filter_match) l 
		left join date_dimension d on l.series_date = d.date
	) d using (series_id)
ON CONFLICT (series_id, team_id, date_key) DO UPDATE
SET
	draw_count =  EXCLUDED.draw_count,
	win_count =  EXCLUDED.win_count,
	lost_count = EXCLUDED.lost_count
	