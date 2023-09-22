INSERT INTO play_fact
	SELECT 
		account_id, 
		CASE WHEN isradiant = True THEN radiant_team_id WHEN isradiant = False THEN dire_team_id END AS team_id, 
		hero_id,
		date_key,
		match_id,
		CASE WHEN isradiant = True THEN 1 WHEN isradiant = False THEN 2 END AS map_id, 
		win as win_count, 
		lose as lose_count,
		kills, 
		deaths as death,
		assists as assist,
		gold_per_min, 
		xp_per_min,
		last_hits as last_hit,
		denies as deny_hit, 
		hero_damage

	FROM match_stage
	LEFT JOIN
		(SELECT match_id, d.date_key
		FROM league_match_stage l 
		LEFT JOIN 
		date_dimension d on d.date = date(l.start_time)) d USING (match_id)
ON CONFLICT (account_id, match_id) DO UPDATE
SET
	team_id = EXCLUDED.team_id, 
	hero_id = EXCLUDED.hero_id,
	date_key = EXCLUDED.date_key,
	map_id = EXCLUDED.map_id, 
	win_count = EXCLUDED.win_count, 
	lose_count = EXCLUDED.lose_count,
	kills = EXCLUDED.kills, 
	death = EXCLUDED.death,
	assist = EXCLUDED.assist,
	gold_per_min = EXCLUDED.gold_per_min, 
	xp_per_min = EXCLUDED.xp_per_min,
	last_hit = EXCLUDED.last_hit,
	deny_hit = EXCLUDED.deny_hit, 
	hero_damage = EXCLUDED.hero_damage
	
