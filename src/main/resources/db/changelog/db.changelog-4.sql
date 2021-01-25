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
        player_position TEXT,
        points          DOUBLE PRECISION
    )
    LANGUAGE plpgsql
AS
$$
BEGIN
    SELECT DISTINCT ON (p.position) (p.first_name || p.last_name) AS "full_name",
                                    a.player_id,
                                    p.position,
                                    a.points
    FROM averages AS a
             JOIN players AS p ON a.player_id = p.id
    ORDER BY p.position, a.points DESC;
    RETURN points;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION
    top_cities()
    RETURNS TABLE (
        team_id BIGINT,
        city    TEXT,
        points  INTEGER
    )
    LANGUAGE plpgsql
AS
$$
BEGIN
    SELECT s.team_id, t.city, SUM(s.points)
    FROM stats AS s
             JOIN teams t ON s.team_id = t.id
    ORDER BY s.points DESC
    LIMIT 10;
    RETURN points;
END;
$$ LANGUAGE plpgsql;

--Players who play more than 20 ‘(Average) for center position, sorted(percentage of freethrow)G
CREATE OR REPLACE FUNCTION
    center_player()
    RETURNS TABLE
    (
        minutes               TEXT,
        first_name            TEXT,
        last_name             TEXT,
        position              TEXT,
        free_throw_percentage DOUBLE PRECISION
    )
    LANGUAGE plpgsql
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
        WHERE averages.minutes < ('20:00')
          AND p.position = 'C'
        ORDER BY averages.free_throw_percentage;

END;
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
    SELECT a.player_id,
           p.height_inches,
           a.three_pointers_made
    FROM averages AS a
             JOIN players AS p ON a.player_id = p.id
    GROUP BY p.height_inches;
END;
$$ LANGUAGE plpgsql;

-- Sorting by division show player name  with more than 15 points and 7  average assist G
CREATE OR REPLACE FUNCTION
    sort_by_division()
    RETURNS TABLE
    (
        points     DOUBLE PRECISION,
        assists    DOUBLE PRECISION,
        first_name TEXT,
        last_name  TEXT,
        division   TEXT
    )
    LANGUAGE plpgsql
AS
$$
BEGIN
    RETURN QUERY
        SELECT averages.points, averages.assists, p.first_name, p.last_name, t.division
        FROM averages
                 JOIN players p ON averages.player_id = p.id
                 JOIN teams t ON p.team_id = t.id
        WHERE averages.points >= (15)
          AND averages.assists >= (7)
        ORDER BY t.division;
END;
$$;

-- Find a games in which the number of turnovers of the winner team is more than the turnovers of  looser team.  V
--this is INCORRECT!
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

    SELECT team_name, sum_of_turnovers = sum_of_turnovers + turnovers;

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
CREATE OR REPLACE FUNCTION
    show_specific_players()
    RETURNS TABLE
    (
        game_id            BIGINT,
        points             DOUBLE PRECISION,
        assists            DOUBLE PRECISION,
        season             INTEGER,
        home_team_score    INTEGER,
        visitor_team_score INTEGER,
        home_team_name     TEXT,
        visitor_team_name  TEXT,
        winner_team_id     BIGINT,
        home_team_id       BIGINT,
        visitor_team_id    BIGINT
    )
    LANGUAGE plpgsql
AS
$$
DECLARE
    -- does not have conflict with parameters
    v_winner BOOL; --1 has won the match, 0 hasn't won the match

BEGIN
    RETURN QUERY
        SELECT game_id,
               points,
               assists,
               season,
               home_team_score,
               visitor_team_score,
               home_team_name,
               visitor_team_name,
               winner_team_id,
               home_team_id,
               visitor_team_id,
               CASE
                   WHEN team_id == winner_team_id
                       THEN v_winner = 1
                   WHEN team_id != winner_team_id
                       THEN v_winner = 0
                   END v_winner
        FROM stats
        WHERE points >= 10
          AND assists >= 10
          AND v_winner = 1
        ORDER BY points;
END;
$$;

-- Compare how many minutes and threepointers attempted per game have done the top 10 players with more than 38% three pointer average. G
CREATE OR REPLACE FUNCTION
    show_best_3_pt()
    RETURNS TABLE
    (
        minutes                  TEXT,
        three_pointer_percentage DOUBLE PRECISION,
        firs_name                TEXT,
        last_name                TEXT,
        team_name                TEXT,
        three_pointer_attempted  INTEGER,
        three_pointer_made       INTEGER
    )
    LANGUAGE plpgsql
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
        WHERE averages.three_pointer_percentage >= 38
        ORDER BY averages.three_pointer_percentage
        LIMIT 10;


END;
$$;

-- How many times a team win in the last month of regular season March and the best scorer for each game
CREATE OR REPLACE FUNCTION
    How_a_team_changes()
    RETURNS TABLE
    (
        season            INTEGER,
        team_full_name    TEXT,
        times_Win         INTEGER,
        date_of_match     date,
        game_id           BIGINT,
        player_first_name TEXT,
        player_last_name  TEXT,
        most_points       INTEGER
    )
    LANGUAGE plpgsql
AS
$$
DECLARE
    times_Win INTEGER;
BEGIN


    RETURN QUERY
        SELECT season,
               date_of_match,
               game_id,
               points,
               p.first_name,
               p.last_name,
               CASE
                   WHEN stats.team_id == winner_team_id
                       THEN times_Win = times_Win + 1
                   END times_Win

        FROM stats
                 JOIN players p ON p.id = stats.player_id
        WHERE date_of_match >= '2015-03-01'
          AND date_of_match <= '2015-04-01'
          AND points = (SELECT MAX(points) FROM stats);
END;
$$
