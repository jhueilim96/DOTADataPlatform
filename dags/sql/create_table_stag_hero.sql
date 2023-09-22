DROP TABLE dota.hero_stag;
DROP TABLE dota.hero_role_stag;

CREATE TABLE dota.hero_stag (
    id INTEGER,
    name VARCHAR(255),
    localized_name VARCHAR(255),
    primary_attr VARCHAR(10),
    attack_type VARCHAR(10),
    legs INTEGER
);

CREATE TABLE dota.hero_role_stag (
    sk SERIAL PRIMARY KEY,
    id INTEGER,
    role VARCHAR(20)
);
