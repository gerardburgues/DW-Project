CREATE OR REPLACE FUNCTION test_fun_avg(p_player_id BIGINT)
    RETURNS DOUBLE PRECISION AS
$$
DECLARE
    v_avg_three_pm DOUBLE PRECISION;
BEGIN
    SELECT AVG(three_pointers_made)
    INTO v_avg_three_pm
    FROM stats
    WHERE player_id = p_player_id
    GROUP BY player_id;

    RETURN v_avg_three_pm;
END;
$$ LANGUAGE plpgsql;
-------
create or replace function
    sum_of_gained_points(team_id bigint, season integer)
    returns integer as $$
declare
    sum_of_points integer;
begin
    select
        sum(stats.points),
        t.name,
        g.season
    into sum_of_points
    from stats join teams t on t.id = stats.team_id
    join games g on stats.game_id = g.id
order by g.date_of_match desc limit 12;
    return sum_of_points;
    end;
$$ language plpgsql;
-------
--! For all the player in the league. I wanna show the player with most average points for each position. V
-- Top 10 cities with the highest points (use sort) V
-- Players who play more than 20 ‘(Average) for  cetnter position, sorted(percentage of freethrow)G
-- Correlation between height of player and how many 3-pointers he made V
-- Sorting by division show player name  with more than 15 points and 7 assist G
-- Find a games in which the number of turnovers of the winner team is more than the turnovers of  looser team.  V
-- Show the games and players in which players has at least 10 assists, 10 points and has won a match (player were in winning team)G
-- Compare how many minutes and threepointers attempted per game have done the top 10 players with more than 38% three pointer. G