INSERT INTO dota.league_stag (leagueid, ticket, banner, tier, name)
VALUES %s
ON CONFLICT (leagueid) DO UPDATE
SET 
    ticket = EXCLUDED.ticket, 
    banner = EXCLUDED.banner, 
    tier = EXCLUDED.tier,
    name = EXCLUDED.name