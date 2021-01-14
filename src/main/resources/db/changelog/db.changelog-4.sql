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
