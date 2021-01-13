ALTER TABLE players
    RENAME COLUMN player_position TO position;

ALTER TABLE players
    ADD FOREIGN KEY (team_id) REFERENCES teams (id) MATCH FULL;

ALTER TABLE games
    ADD FOREIGN KEY (home_team_id) REFERENCES teams (id) MATCH FULL,
    ADD FOREIGN KEY (visitor_team_id) REFERENCES teams (id) MATCH FULL;

ALTER TABLE stats
    ADD FOREIGN KEY (player_id) REFERENCES players (id) MATCH FULL,
    ADD FOREIGN KEY (team_id) REFERENCES teams (id) MATCH FULL,
    ADD FOREIGN KEY (game_id) REFERENCES games (id) MATCH FULL;
