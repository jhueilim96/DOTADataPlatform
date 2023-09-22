CREATE TABLE IF NOT EXISTS dota.match_details_stag(
    "match_details_sk" INT GENERATED ALWAYS AS IDENTITY NOT NULL,
    "match_id" bigint,
    "barracks_status_dire" int,
    "barracks_status_radiant" int,
    "cluster" int,
    "dire_score" int,
    "dire_team_id" bigint,
    "duration" int,
    "engine" int,
    "first_blood_time" int,
    "game_mode" int,
    "human_players" int,
    "leagueid" bigint,
    "lobby_type" int,
    "match_seq_num" bigint,
    "negative_votes" int,
    "positive_votes" int,
    "radiant_score" int,
    "radiant_team_id" bigint,
    "radiant_win" bool,
    "skill" VARCHAR,
    "start_time" bigint,
    "tower_status_dire" int,
    "tower_status_radiant" int,
    "version" int,
    "replay_salt" bigint,
    "series_id" bigint,
    "series_type" int,
    "patch" int,
    "region" int,
    "comeback" int,
    "stomp" int,
    "replay_url" VARCHAR,
    "chat" jsonb,
    "cosmetics" jsonb,
    "draft_timings" jsonb,
    "objectives" jsonb,
    "picks_bans" jsonb,
    "radiant_gold_adv" jsonb,
    "radiant_xp_adv" jsonb,
    "teamfights" jsonb,
    "league" jsonb,
    "radiant_team" jsonb,
    "dire_team" jsonb,
    "players" jsonb,
    "all_word_counts" jsonb,
    "my_word_counts" jsonb,
    "created" TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    "modified" TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);


ALTER TABLE IF EXISTS dota.match_details_stag
    ADD CONSTRAINT match_league_details_unique UNIQUE (match_id);