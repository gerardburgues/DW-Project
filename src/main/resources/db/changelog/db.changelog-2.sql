DROP TABLE teams;

CREATE TABLE IF NOT EXISTS teams (
    id           BIGINT PRIMARY KEY UNIQUE,
    abbreviation TEXT NOT NULL,
    city         TEXT NOT NULL,
    conference   TEXT NOT NULL,
    division     TEXT NOT NULL,
    full_name    TEXT NOT NULL,
    name         TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS players (
    id              BIGINT PRIMARY KEY UNIQUE,
    first_name      TEXT   NOT NULL,
    last_name       TEXT   NOT NULL,
    player_position TEXT   NOT NULL,
    height_feet     INTEGER,
    height_inches   INTEGER,
    weight_pounds   INTEGER,
    team_id         BIGINT NOT NULL
);

CREATE TABLE IF NOT EXISTS games (
    id                 BIGINT PRIMARY KEY UNIQUE,
    date_of_match      DATE    NOT NULL,
    home_team_score    INTEGER NOT NULL,
    visitor_team_score INTEGER NOT NULL,
    season             INTEGER NOT NULL,
    period             INTEGER NOT NULL,
    status             TEXT    NOT NULL,
    time_till_start    TEXT    NOT NULL,
    postseason         BOOLEAN NOT NULL,
    home_team_id       BIGINT  NOT NULL,
    visitor_team_id    BIGINT  NOT NULL
);

CREATE TABLE IF NOT EXISTS stats (
    id                       BIGINT PRIMARY KEY UNIQUE,
    player_id                BIGINT,
    team_id                  BIGINT,
    game_id                  BIGINT,
    "minutes"                TEXT,
    points                   INTEGER,
    assists                  INTEGER,
    rebounds                 INTEGER,
    defensive_rebounds       INTEGER,
    offensive_rebounds       INTEGER,
    blocks                   INTEGER,
    steals                   INTEGER,
    turnovers                INTEGER,
    personal_fouls           INTEGER,
    field_goals_attempted    INTEGER,
    field_goals_made         INTEGER,
    field_goal_percentage    DOUBLE PRECISION,
    three_pointers_attempted INTEGER,
    three_pointers_made      INTEGER,
    three_pointer_percentage DOUBLE PRECISION,
    free_throws_attempted    INTEGER,
    free_throws_made         INTEGER,
    free_throw_percentage    DOUBLE PRECISION
);

CREATE TABLE IF NOT EXISTS averages (
    player_id                BIGINT,
    season                   INTEGER,
    games_played             INTEGER,
    "minutes"                TEXT,
    points                   DOUBLE PRECISION,
    assists                  DOUBLE PRECISION,
    rebounds                 DOUBLE PRECISION,
    defensive_rebounds       DOUBLE PRECISION,
    offensive_rebounds       DOUBLE PRECISION,
    blocks                   DOUBLE PRECISION,
    steals                   DOUBLE PRECISION,
    turnovers                DOUBLE PRECISION,
    personal_fouls           DOUBLE PRECISION,
    field_goals_attempted    DOUBLE PRECISION,
    field_goals_made         DOUBLE PRECISION,
    field_goal_percentage    DOUBLE PRECISION,
    three_pointers_attempted DOUBLE PRECISION,
    three_pointers_made      DOUBLE PRECISION,
    three_pointer_percentage DOUBLE PRECISION,
    free_throws_attempted    DOUBLE PRECISION,
    free_throws_made         DOUBLE PRECISION,
    free_throw_percentage    DOUBLE PRECISION
);
