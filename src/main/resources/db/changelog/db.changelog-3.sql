Alter table players_game
    add housescore int,
    add visitscore int,
    drop column winner;

Alter table player_avg
    drop column first_name,
    drop column last_name,
    drop column asspg;