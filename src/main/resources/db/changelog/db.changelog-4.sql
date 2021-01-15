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
