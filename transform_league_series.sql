with league_srs as (
	select distinct series_id, leagueid, series_type
	FROM league_match_stage
) 
INSERT INTO league_series (series_id, league_name, league_tier, series_type)
select 
	s.series_id as series_id, 
	l.name as league_name, 
	l.tier as league_tier, 
	s.series_type as series_type
FROM league_srs s
LEFT JOIN league_temp l on s.leagueid=l.league_id