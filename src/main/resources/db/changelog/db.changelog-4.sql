-- For all the player in the league. I wanna show the player with most average points for each position. V
-- Top 10 cities with the highest points (use sort) V
-- Players who play more than 20 ‘(Average) for center position, sorted(percentage of freethrow)G
-- Correlation between height of player and how many 3-pointers he made V
-- Sorting by division show player name  with more than 15 points and 7 assist G
-- Find games in which the number of turnovers of the winner team is more than the turnovers of  looser team.  V
-- Show the games and players in which players has at least 10 assists, 10 points and has won a match (player were in winning team)G
-- Compare how many minutes and threepointers attempted per game have done the top 10 players with more than 38% three pointer. G

CREATE OR REPLACE FUNCTION find_winner_id(p_game_id BIGINT) RETURNS BIGINT AS
$$
DECLARE
    result BIGINT;
BEGIN
    SELECT CASE
               WHEN home_team_score > visitor_team_score THEN home_team_id
               ELSE visitor_team_id
               END
    INTO result
    FROM games
    WHERE id = p_game_id;
    RETURN result;
END;
$$ LANGUAGE plpgsql;

ALTER TABLE games
    RENAME COLUMN time_till_start TO time;

ALTER TABLE games
    RENAME COLUMN date_of_match TO date;

ALTER TABLE stats
    ADD COLUMN home_team_score    INTEGER,
    ADD COLUMN visitor_team_score INTEGER,
    ADD COLUMN season             INTEGER,
    ADD COLUMN date               DATE,
    ADD COLUMN first_name         TEXT,
    ADD COLUMN last_name          TEXT;

UPDATE stats
SET home_team_score    = games.home_team_score,
    visitor_team_score = games.visitor_team_score,
    season             = games.season,
    date               = games.date
FROM games
WHERE stats.game_id = games.id;

UPDATE stats
SET first_name = players.first_name,
    last_name  = players.last_name
FROM players
WHERE stats.player_id = players.id;

ALTER TABLE stats
    ALTER COLUMN home_team_score SET NOT NULL,
    ALTER COLUMN visitor_team_score SET NOT NULL,
    ALTER COLUMN season SET NOT NULL,
    ALTER COLUMN date SET NOT NULL,
    ALTER COLUMN first_name SET NOT NULL,
    ALTER COLUMN last_name SET NOT NULL;

ALTER TABLE stats
    ADD COLUMN home_team_id      BIGINT,
    ADD COLUMN visitor_team_id   BIGINT,
    ADD COLUMN winner_team_id    BIGINT,
    ADD COLUMN home_team_name    TEXT,
    ADD COLUMN visitor_team_name TEXT;

UPDATE stats
SET home_team_id    = games.home_team_id,
    visitor_team_id = games.visitor_team_id,
    winner_team_id  = games.winner_team_id
FROM games
WHERE stats.game_id = games.id;

UPDATE stats
SET home_team_name = name
FROM teams
WHERE stats.home_team_id = teams.id;

UPDATE stats
SET visitor_team_name = name
FROM teams
WHERE stats.visitor_team_id = teams.id;

ALTER TABLE stats
    ALTER COLUMN home_team_id SET NOT NULL,
    ALTER COLUMN visitor_team_id SET NOT NULL,
    ALTER COLUMN winner_team_id SET NOT NULL,
    ALTER COLUMN home_team_name SET NOT NULL,
    ALTER COLUMN visitor_team_name SET NOT NULL,
    ADD FOREIGN KEY (visitor_team_id) REFERENCES teams (id) MATCH FULL,
    ADD FOREIGN KEY (winner_team_id) REFERENCES teams (id) MATCH FULL,
    ADD FOREIGN KEY (home_team_id) REFERENCES teams (id) MATCH FULL;

ALTER TABLE games
    ADD COLUMN winner_team_id BIGINT;

UPDATE games
SET winner_team_id = find_winner_id(id)
WHERE TRUE;


ALTER TABLE games
    ALTER COLUMN winner_team_id SET NOT NULL,
    ADD FOREIGN KEY (winner_team_id) REFERENCES teams (id) MATCH FULL;

ALTER TABLE stats
    DROP COLUMN home_team_name,
    DROP COLUMN visitor_team_name;

CREATE TABLE etl_status (
    table_name TEXT,
    done       BOOL
);

ALTER TABLE stats
    ALTER COLUMN id SET NOT NULL,
    ALTER COLUMN player_id SET NOT NULL,
    ALTER COLUMN team_id SET NOT NULL,
    ALTER COLUMN game_id SET NOT NULL
;

CREATE OR REPLACE FUNCTION
    best_player()
    RETURNS TABLE
    (
        player_id       BIGINT,
        first_name      TEXT,
        last_name       TEXT,
        position        TEXT,
        points          DOUBLE PRECISION
    )
    LANGUAGE plpgsql
AS
$$
BEGIN
    return query
    SELECT DISTINCT ON (p.position) (p.first_name || p.last_name) AS "full_name",
                                        a.player_id,
                                        p.position,
                                        a.points
        FROM averages AS a
                 JOIN players AS p ON a.player_id = p.id
        ORDER BY p.position, a.points DESC;
END;
$$;

CREATE OR REPLACE FUNCTION
    top_cities()
    RETURNS TABLE
            (
                team_id BIGINT,
                city    TEXT,
                points  INTEGER
            )
    LANGUAGE plpgsql
AS
$$
BEGIN
    return query
    SELECT s.team_id, t.city, SUM(s.points)
    FROM stats AS s
             JOIN teams t ON s.team_id = t.id
    ORDER BY s.points DESC
    LIMIT 10;
END;
$$;

--Players who play more than 20 ‘(Average) for center position, sorted(percentage of freethrow)G
create function center_player(s_minutes text, s_position text)
    returns TABLE
            (
                minutes               text,
                first_name            text,
                last_name             text,
                v_position            text,
                free_throw_percentage double precision
            )
    language plpgsql
as
$$
BEGIN
    RETURN QUERY
        SELECT averages.minutes,
               p.first_name,
               p.last_name,
               p.position,
               averages.free_throw_percentage
        FROM averages
                 JOIN players p ON p.id = averages.player_id
        WHERE averages.minutes > (s_minutes)
          AND p.position = s_position
        ORDER BY averages.minutes, averages.free_throw_percentage;

END
$$;

-- Correlation between height of player and how many 3-pointers he made V
CREATE OR REPLACE FUNCTION
    corr_height_player()
    RETURNS TABLE
    (
        player_id           BIGINT,
        height_inches       INTEGER,
        three_pointers_made DOUBLE PRECISION
    )
    LANGUAGE plpgsql
AS
$$
BEGIN
    return query
    SELECT a.player_id,
           p.height_inches,
           a.three_pointers_made
    FROM averages AS a
             JOIN players AS p ON a.player_id = p.id
    GROUP BY p.height_inches;
END;
$$ LANGUAGE plpgsql;

-- Sorting by division show player name  with more than 15 points and 7  average assist G
create function sort_by_division(s_points integer, s_assists integer)
    returns TABLE
            (
                season     integer,
                points     double precision,
                assists    double precision,
                first_name text,
                last_name  text,
                division   text
            )
    language plpgsql
as
$$
BEGIN
    RETURN QUERY
        SELECT averages.season, averages.points, averages.assists, p.first_name, p.last_name, t.division
        FROM averages
                 JOIN players p ON averages.player_id = p.id

                 JOIN teams t ON p.team_id = t.id
        WHERE averages.points >= (s_points)
          AND averages.assists >= (s_assists)
        ORDER BY t.division;
END;
$$;

-- Find a games in which the number of turnovers of the winner team is more than the turnovers of  looser team.  V
CREATE OR REPLACE FUNCTION
    turnover_stat()
    RETURNS TABLE
    (
        id        BIGINT,
        turnovers DOUBLE PRECISION,
        name      TEXT,
        points    DOUBLE PRECISION
    )
    LANGUAGE plpgsql
AS
$$
DECLARE
    sum_of_turnovers INTEGER;
BEGIN

--SELECT team_name, sum_of_turnovers = sum_of_turnovers + turnovers;

    -- for teamA Calculate for each player how many turnovers
    -- for teamB Calculate for each player how many turnovers
    -- if teamA turnovers > teamB turnovers && teamA won
    -- Add teamA and numbers of turnovers
    -- if teamB turnovers > teamA turnovers && teamB won
    --Add teamB and number of turnovers

    -- from stats

END;
$$;

-- Show the games and players in which players has at least 10 assists, 10 points and has won a match (player were in winning team)G
create function show_specific_players(s_points integer, s_assits integer)
    returns TABLE
            (
                v_game_id            bigint,
                first_name           text,
                last_name            text,
                v_points             integer,
                v_assists            integer,
                v_season             integer,
                v_home_team_score    integer,
                v_visitor_team_score integer,
                home_team_name       text,
                visitor_team_name    text,
                v_winner_team_id     bigint,
                v_home_team_id       bigint,
                v_visitor_team_id    bigint
            )
    language plpgsql
as
$$
DECLARE
    -- does not have conflict with parameters

    home_team_name    text;
    visitor_team_name text;

BEGIN
    RETURN QUERY
        SELECT game_id,
               COALESCE(stats.first_name, '0'),
               COALESCE(stats.last_name, '0'),
               points,
               assists,
               COALESCE(season, 0),
               COALESCE(home_team_score, 0),
               COALESCE(visitor_team_score, 0),
               t1.name,
               t2.name,
               COALESCE(winner_team_id, 0000),
               COALESCE(home_team_id, 0000),
               COALESCE(visitor_team_id, 0000)


        FROM stats
                 join teams t1 on t1.id = stats.home_team_id
                 join teams t2 on t2.id = stats.visitor_team_id
        WHERE points >= s_points
          AND assists >= s_assits
          AND winner_team_id = team_id
        ORDER BY points;
END;
$$;
-- Compare how many minutes and threepointers attempted per game have done the top 10 players with more than 38% three pointer average. G
create function show_best_3_pt(a_percentage double precision)
    returns TABLE
            (
                season                   integer,
                minutes                  text,
                three_pointer_percentage double precision,
                firs_name                text,
                last_name                text,
                team_name                text,
                three_pointer_attempted  double precision,
                three_pointer_made       double precision
            )
    language plpgsql
as
$$
BEGIN
    RETURN QUERY
        SELECT averages.season,
               averages.minutes,
               averages.three_pointer_percentage,
               p.first_name,
               p.last_name,
               t.name,
               averages.three_pointers_attempted,
               averages.three_pointers_made
        FROM averages
                 JOIN players p ON averages.player_id = p.id
                 JOIN teams t ON p.team_id = t.id

        WHERE averages.three_pointer_percentage >= a_percentage
        group by averages.season, p.first_name, averages.minutes, averages.three_pointer_percentage, p.last_name,
                 t.name, averages.three_pointers_attempted, averages.three_pointers_made
        ORDER BY averages.three_pointer_percentage;


END;
$$;

-- How many times a team win at home in the last month of regular season March and the players
create function how_a_team_changes(date1 date, date2 date)
    returns TABLE
            (
                team_full_name text,
                season         integer,
                date11         date,
                date12         date,
                number         bigint
            )
    language plpgsql
as
$$
DECLARE

BEGIN

    RETURN QUERY
        SELECT t.name,
               games.season,
               date1,
               date2,
               COUNT(*)

        FROM games

                 join stats s on games.id = s.game_id
                 join teams t on t.id = games.home_team_id

        WHERE games.date >= date1
          AND games.date <= date2
          AND games.home_team_id = games.winner_team_id

        group by games.season, t.name, games.home_team_id, games.winner_team_id;

END;
$$;
