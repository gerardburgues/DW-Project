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
$$ LANGUAGE PLPGSQL;

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
    ALTER COLUMN game_id SET NOT NULL;

CREATE OR REPLACE FUNCTION best_player()
    RETURNS TABLE
    (
        player_id  BIGINT,
        first_name TEXT,
        last_name  TEXT,
        position   TEXT,
        points     DOUBLE PRECISION
    )
AS
$$
BEGIN
    RETURN QUERY
        SELECT DISTINCT ON (p.position) (p.first_name || ' ' || p.last_name) AS "full_name",
                                        p.position,
                                        s.points
        FROM stats AS s
                 JOIN players AS p ON s.player_id = p.id
        GROUP BY (p.first_name || ' ' || p.last_name), p.position, s.points
        ORDER BY p.position, s.points DESC;
END;
$$ LANGUAGE PLPGSQL;

CREATE OR REPLACE FUNCTION top_cities()
    RETURNS TABLE
    (
        team_id BIGINT,
        city    TEXT,
        points  INTEGER
    )
AS
$$
BEGIN
    RETURN QUERY
        SELECT s.team_id, t.city, SUM(s.points)
        FROM stats AS s
                 JOIN teams t ON s.team_id = t.id
        GROUP BY s.team_id, s.points, t.city
        ORDER BY s.points DESC
        LIMIT 10;
END;
$$ LANGUAGE PLPGSQL;

--Players who play more than 20 ‘(Average) for center position, sorted(percentage of freethrow)G
CREATE FUNCTION center_player(s_minutes TEXT, s_position TEXT)
    RETURNS TABLE
    (
        minutes               TEXT,
        first_name            TEXT,
        last_name             TEXT,
        v_position            TEXT,
        free_throw_percentage DOUBLE PRECISION
    )
AS
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
        WHERE averages.minutes < (s_minutes)
          AND p.position = s_position
        ORDER BY averages.free_throw_percentage;

END
$$ LANGUAGE PLPGSQL;

-- Correlation between height of player and how many 3-pointers he made V
CREATE OR REPLACE FUNCTION
    corr_height_player()
    RETURNS TABLE
    (
        first_name          TEXT,
        last_name           TEXT,
        height_inches       INTEGER,
        three_pointers_made DOUBLE PRECISION,
        position            TEXT
    )
AS
$$
BEGIN
    RETURN QUERY
        SELECT p.first_name,
               p.last_name,
               p.height_inches,
               s.three_pointers_made,
               p.position
        FROM stats AS s
                 JOIN players AS p ON s.player_id = p.id
        WHERE p.height_inches > 0
        GROUP BY p.first_name,
                 p.last_name, p.height_inches, p.position, p.height_inches, s.three_pointers_made
        ORDER BY p.height_inches, s.three_pointers_made;
END;
$$ LANGUAGE PLPGSQL;

-- Sorting by division show player name  with more than 15 points and 7  average assist G
CREATE FUNCTION sort_by_division(s_points INTEGER, s_assists INTEGER)
    RETURNS TABLE
    (
        points     DOUBLE PRECISION,
        assists    DOUBLE PRECISION,
        first_name TEXT,
        last_name  TEXT,
        division   TEXT
    )
AS
$$
BEGIN
    RETURN QUERY
        SELECT averages.points, averages.assists, p.first_name, p.last_name, t.division
        FROM averages
                 JOIN players p ON averages.player_id = p.id
                 JOIN teams t ON p.team_id = t.id
        WHERE averages.points >= (s_points)
          AND averages.assists >= (s_assists)
        ORDER BY t.division;
END;
$$ LANGUAGE PLPGSQL;

-- Show the games and players in which players has at least 10 assists, 10 points and has won a match (player were in winning team)G
CREATE FUNCTION show_specific_players(s_points INTEGER, s_assits INTEGER)
    RETURNS TABLE
    (
        v_game_id            BIGINT,
        first_name           TEXT,
        last_name            TEXT,
        v_points             INTEGER,
        v_assists            INTEGER,
        v_season             INTEGER,
        v_home_team_score    INTEGER,
        v_visitor_team_score INTEGER,
        home_team_name       TEXT,
        visitor_team_name    TEXT,
        v_winner_team_id     BIGINT,
        v_home_team_id       BIGINT,
        v_visitor_team_id    BIGINT
    )
AS
$$
BEGIN
    RETURN QUERY
        SELECT game_id,
               stats.first_name,
               stats.last_name,
               points,
               assists,
               season,
               home_team_score,
               visitor_team_score,
               t1.name,
               t2.name,
               winner_team_id,
               home_team_id,
               visitor_team_id
        FROM stats
                 JOIN teams t1 ON t1.id = stats.home_team_id
                 JOIN teams t2 ON t2.id = stats.visitor_team_id
        WHERE points >= s_points
          AND assists >= s_assits
          AND winner_team_id = team_id
        ORDER BY points;
END;
$$ LANGUAGE PLPGSQL;

-- Compare how many minutes and threepointers attempted per game have done the top 10 players with more than 38% three pointer average. G
CREATE FUNCTION show_best_3_pt(a_percentage DOUBLE PRECISION)
    RETURNS TABLE
    (
        season                   INTEGER,
        minutes                  TEXT,
        three_pointer_percentage DOUBLE PRECISION,
        firs_name                TEXT,
        last_name                TEXT,
        team_name                TEXT,
        three_pointer_attempted  DOUBLE PRECISION,
        three_pointer_made       DOUBLE PRECISION
    )
AS
$$
BEGIN
    RETURN QUERY
        SELECT averages.minutes,
               averages.three_pointer_percentage,
               p.first_name,
               p.last_name,
               t.name,
               s.three_pointers_attempted,
               s.three_pointers_made
        FROM averages
                 JOIN players p ON averages.player_id = p.id
                 JOIN teams t ON p.team_id = t.id
                 JOIN stats s ON p.id = s.player_id
        WHERE averages.three_pointer_percentage >= a_percentage
        GROUP BY averages.season, p.first_name, averages.minutes, averages.three_pointer_percentage,
                 p.last_name, t.name, averages.three_pointers_attempted, averages.three_pointers_made
        ORDER BY averages.three_pointer_percentage;
END;
$$ LANGUAGE PLPGSQL;

-- How many times a team win at home in the last month of regular season March and the players
CREATE FUNCTION how_a_team_changes(date1 DATE, date2 DATE)
    RETURNS TABLE
    (
        team_full_name TEXT,
        season         INTEGER,
        date11         DATE,
        date12         DATE,
        number         BIGINT
    )
AS
$$
BEGIN
    RETURN QUERY
        SELECT t.name,
               date1,
               date2,
               COUNT(*)
        FROM games
                 JOIN stats s ON games.id = s.game_id
                 JOIN teams t ON t.id = s.home_team_id
        WHERE games.date >= date1
          AND games.date <= date2
          AND games.home_team_id = games.winner_team_id
        GROUP BY games.season, t.name, games.home_team_id, games.winner_team_id;
END;
$$ LANGUAGE PLPGSQL;
