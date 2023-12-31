CREATE TABLE IF NOT EXISTS dota.match_league_stag(
    "match_league_sk" INT GENERATED ALWAYS AS IDENTITY NOT NULL,
    "match_id" bigint NOT NULL,
    "match_seq_num" bigint,
    "radiant_win" bool,
    "start_time" bigint,
    "duration" int,
    "tower_status_radiant" int,
    "tower_status_dire" int,
    "barracks_status_radiant" int,
    "barracks_status_dire" int,
    "cluster" int,
    "first_blood_time" int,
    "lobby_type" int,
    "human_players" int,
    "leagueid" bigint NOT NULL,
    "positive_votes" int,
    "negative_votes" int,
    "game_mode" int,
    "engine" int,
    "radiant_team_id" bigint NOT NULL,
    "dire_team_id" bigint NOT NULL,
    "radiant_team_name" VARCHAR,
    "dire_team_name" VARCHAR,
    "radiant_team_complete" int,
    "dire_team_complete" int,
    "radiant_captain" bigint,
    "dire_captain" bigint,
    "version" int,
    "radiant_score" int,
    "dire_score" int,
    "series_id" bigint NOT NULL,
    "series_type" int,
    "picks_bans" JSONB,
    "chat" JSONB,
    "objectives" JSONB,
    "radiant_gold_adv" JSONB,
    "radiant_xp_adv" JSONB,
    "teamfights" JSONB,
    "cosmetics" JSONB,
    "draft_timings" JSONB,
    "created" TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    "modified" TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

ALTER TABLE IF EXISTS dota.match_league_stag
    ADD CONSTRAINT match_league_stag_unique UNIQUE (match_id);