-- For all the player in the league. I wanna show the player with most average points for each position. V
-- Top 10 cities with the highest points (use sort) V
-- Players who play more than 20 ‘(Average) for center position, sorted(percentage of freethrow)G
-- Correlation between height of player and how many 3-pointers he made V
-- Sorting by division show player name  with more than 15 points and 7 assist G
-- Find games in which the number of turnovers of the winner team is more than the turnovers of  looser team.  V
-- Show the games and players in which players has at least 10 assists, 10 points and has won a match (player were in winning team)G
-- Compare how many minutes and threepointers attempted per game have done the top 10 players with more than 38% three pointer. G
--ALTER STATEMENTS FOR ADDING NEW COLUMNS
ALTER TABLE stats
    add column home_team_score    integer NOT NULL,
    add column visitor_team_score integer NOT NULL,
    add column season             integer NOT NULL,
    add column date_of_match      date    NOT NULL,
    add column home_team_id       bigint  NOT NULL,
    add column visitor_team_id    bigint  NOT NULL,
    add column winner_team_id     bigint  NOT NULL;

ALTER TABLE stats
    add column home_team_name    text,
    add column visitor_team_name text;

CREATE OR REPLACE FUNCTION
    best_player()
    RETURNS TABLE
    (
        player_id BIGINT,
        first_name TEXT,
        last_name TEXT,
        player_position TEXT,
        points DOUBLE PRECISION
    )
    language plpgsql
    as
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
    RETURN points;
END;
$$ ;

CREATE OR REPLACE FUNCTION
    top_cities()
    RETURNS table (
        team_id BIGINT,
        city TEXT,
        points INTEGER
                  )
    LANGUAGE plpgsql
    as
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
    RETURNS table
            (
                minutes               text,
                first_name            text,
                last_name             text,
                position              text,
                free_throw_percentage double precision
            )
    language plpgsql
as
$$
BEGIN
    return query
        SELECT averages.minutes,
               p.first_name,
               p.last_name,
               p.position,
               averages.free_throw_percentage
        FROM averages
                 JOIN players p ON p.id = averages.player_id
        WHERE averages.minutes < ('20:00')
          AND position = 'C'
        ORDER BY averages.free_throw_percentage;

END;
$$ LANGUAGE plpgsql;

-- Correlation between height of player and how many 3-pointers he made V
CREATE OR REPLACE FUNCTION
    corr_height_player()
    RETURNS table
    (
        player_id BIGINT,
        height_inches INTEGER,
        three_pointers_made DOUBLE PRECISION
    )
    LANGUAGE plpgsql
    as
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
    RETURNS table
            (
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
    return query
        SELECT averages.points, averages.assists, p.first_name, p.last_name, t.division
        FROM averages
                 JOIN players p ON averages.player_id = p.id
                 JOIN teams t ON p.team_id = t.id
        WHERE averages.points >= (15)
          AND averages.assists >= (7)
        ORDER BY t.division;
END;
$$ LANGUAGE plpgsql;

-- Find a games in which the number of turnovers of the winner team is more than the turnovers of  looser team.  V
CREATE OR REPLACE FUNCTION
    turnover_stat()
    RETURNS table
    (
        id BIGINT,
        turnovers DOUBLE PRECISION,
        name TEXT,
        points DOUBLE PRECISION
    )
    LANGUAGE plpgsql
    as
$$
declare sum_of_turnovers integer;
BEGIN
    SELECT t.name, sum(),
    union all
    select count(turnovers)
    CASE
    --for teamA for each player how many turnovers
    select
    --for teamB for each player how many turnovers
    select sum(count(turnovers))
        WHEN s.winner_team_id IS NOT NULL
            and
            THEN A
        ELSE B
        END
    FROM games AS g
             JOIN stats s ON g.id = s.game_id
             JOIN teams t ON s.team_id = t.id
    when ;
END;
$$ LANGUAGE plpgsql;

-- Show the games and players in which players has at least 10 assists, 10 points and has won a match (player were in winning team)G
CREATE OR REPLACE FUNCTION
    show_specific_players()
    RETURNS table
            (
                game_id            bigint,
                points             double precision,
                assists            double precision,
                season             integer,
                home_team_score    integer,
                visitor_team_score integer,
                home_team_name     text,
                visitor_team_name  text,
                winner_team_id     bigint,
                home_team_id       bigint,
                visitor_team_id    bigint
            )
    language plpgsql
as
$$
DECLARE
    -- does not have conflict with parameters
    v_winner BOOL; --1 has won the match, 0 hasn't won the match

BEGIN
    return query
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
$$ LANGUAGE plpgsql;

-- Compare how many minutes and threepointers attempted per game have done the top 10 players with more than 38% three pointer average. G

CREATE OR REPLACE FUNCTION
    show_best_3_pt()
    RETURNS table
            (
                minutes                  text,
                three_pointer_percentage double precision,
                firs_name                text,
                last_name                text,
                team_name                text,
                three_pointer_attempted  integer,
                three_pointer_made       integer
            )
    language plpgsql
as
$$
BEGIN
    return query
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
$$ LANGUAGE plpgsql;

-- How many times a team win in the last month of regular season March and the best scorer for each game
CREATE OR REPLACE FUNCTION
    How_a_team_changes(times_Win integer)
    returns table
            (
                season            integer,
                team_full_name    text,
                times_Win         integer,
                date_of_match     date,
                game_id           bigint,
                player_first_name text,
                player_last_name  text,
                most_points       integer
            )
    language plpgsql
as
$$
BEGIN

    Return query
        Select season,
               date_of_match,
               game_id,
               points,
               p.first_name,
               p.last_name,
               CASE
                   WHEN stats.team_id == winner_team_id
                       THEN times_Win = times_Win + 1
                   END times_Win

        from stats
                 join players p on p.id = stats.player_id
        where date_of_match >= '2015-03-01'
          AND date_of_match <= '2015-04-01'
          and points = (select max(points) from stats)
        order by times_Win;
end;
$$