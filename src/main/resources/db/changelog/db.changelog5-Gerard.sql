--
create or replace function
    best_player(player_id bigint, first_name text, last_name text, player_position text, points double precision)
    returns double precision as
$$
begin
    select distinct on (p.player_position)
    (p.first_name || p.last_name) as "full_name",
    a.player_id, p.player_position, a.points
    from averages as a
    join players as p on a.player_id = p.id
    order by p.player_position, a.points desc;
    return points;
    end;
$$ language plpgsql;
--
create or replace function
    top_cities(team_id bigint, city text, points integer)
    returns integer as
$$
begin
    select s.team_id, t.city, sum(s.points)
    from stats as s join teams t on s.team_id = t.id
    order by s.points desc
    limit 10;
    return points;
end;
$$ language plpgsql;
---
create or replace function
    center_player(minutes text, Player_ID bigint, first_name text, last_name text, pos text)
    returns text as
$$
begin
    select averages.minutes,
           p.first_name,
           p.last_name,
           p.player_position == 'C',
           averages.free_throw_percentage
    from averages
             join players p on p.id = averages.player_id
    where averages.minutes < ('20:00')
      and player_position = 'C'
    order by averages.free_throw_percentage;
    return first_name;-- here may change
end;
$$ language plpgsql;

-- Correlation between height of player and how many 3-pointers he made V
create or replace function
    corr_height_player(player_id bigint, height_inches integer, three_pointers_made double precision)
    returns double precision as $$
begin
    select
        a.player_id,
        p.height_inches,
        a.three_pointers_made
    from averages as a join players as p on a.player_id = p.id
    group by p.height_inches;
end;
$$ language plpgsql;

-- Sorting by division show player name  with more than 15 points and 7  average assist G
create or replace function
    sort_by_division(division text, points double precision, assist double precision)
    returns text as
$$

begin
    select averages.points, averages.assists, p.first_name, p.last_name, t.division
    from averages
             join players p on averages.player_id = p.id
             join teams t on p.team_id = t.id
    where averages.points >= (15)
      and averages.assists >= (7)
    order by t.division;
    return points; --here may change
end;
$$ language plpgsql;
-- Find a games in which the number of turnovers of the winner team is more than the turnovers of  looser team.  V
--this is INCORRECT!
create or replace function
    turnover_stat(id bigint, turnovers double precision, name text, points double precision)
    returns double precision as $$
begin
    select
        g.id, s.turnovers, t.name, s.points
    from games as g join stats s on g.id = s.game_id
    join teams t on s.team_id = t.id;
end;
$$ language plpgsql;

-- Show the games and players in which players has at least 10 assists, 10 points and has won a match (player were in winning team)G
create or replace function
    show_specific_players(points double precision, assist double precision)
    returns text as
$$
declare
    winner   bool;
    whoiswho text;
begin
    select stats.game_id,
           stats.points,
           stats.assists,
           g.season,
           g.home_team_score,
           g.visitor_team_score,
           t.name,
           t2.name,

           case
               when stats.team_id = g.visitor_team_score
                   then (case
                             when g.visitor_team_score > g.home_team_score
                                 then winner = 0 -- player has won
                             else winner = 1
                   end)
               when stats.team_id = g.home_team_score
                   then case
                            when g.visitor_team_score > g.home_team_score
                                then winner = 0 -- player has won
                            else winner = 1
                   end
               end as winner
    from stats
             join games g on stats.game_id = g.id
             join teams t on g.visitor_team_id = t.id
             join teams t2 on g.home_team_id = t2.id
    where stats.points >= 10
      and stats.assists >= 10
      and winner = 0;

end;
$$ language plpgsql;

-- Compare how many minutes and threepointers attempted per game have done the top 10 players with more than 38% three pointer average. G

create or replace function
    show_best_3_pt(points double precision, percentage_3 double precision)
    returns text as
$$

begin
    select averages.minutes,
           averages.three_pointer_percentage,
           p.first_name,
           p.last_name,
           t.name,
           s.three_pointers_attempted,
           s.three_pointers_made
    from averages
             join players p on averages.player_id = p.id
             join teams t on p.team_id = t.id
             join stats s on p.id = s.player_id
    where averages.three_pointer_percentage >= 38
    order by averages.three_pointer_percentage
    limit 10;


end;
$$ language plpgsql