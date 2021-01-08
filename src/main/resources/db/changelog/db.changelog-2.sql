/*
CREATE TABLE PLayer_AVG(

    Player_Id int primary key,
    First_Name varchar(50),
    Last_Name varchar(50),
    Season int,
    total_games int,
    minpg int,
    ppg int, /***pg means per game */
    rebpg int,
    astpg int,
    stlpg int,
    blkpg int,
    asspg int



);*/

CREATE TABLE players_game
(
    gameId    int PRIMARY KEY UNIQUE,
    PlayerId  int,
    min       int,
    teamId    varchar,
    firstname varchar,
    lastname  varchar,
    pos       varchar,
    points    int,
    totReb    int,
    assist    int,
    stl       int,
    blocks    int,
    turnovers int,
    /**Win 0 Lose 1**/
    winner    bit,
    FOREIGN key (PlayerId) References Player_AVG (Player_Id)
);