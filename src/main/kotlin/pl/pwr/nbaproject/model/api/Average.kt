package pl.pwr.nbaproject.model.api

import com.fasterxml.jackson.annotation.JsonProperty

data class Average(
    @JsonProperty(PLAYER_ID) val playerId: Long,
    @JsonProperty(SEASON) val season: Int,
    @JsonProperty(GAMES_PLAYED) val gamesPlayed: Int,
    @JsonProperty(MINUTES) val minutes: String,
    @JsonProperty(POINTS) val points: Double,
    @JsonProperty(ASSISTS) val assists: Double,
    @JsonProperty(REBOUNDS) val rebounds: Double,
    @JsonProperty(DEFENSIVE_REBOUNDS) val defensiveRebounds: Double,
    @JsonProperty(OFFENSIVE_REBOUNDS) val offensiveRebounds: Double,
    @JsonProperty(BLOCKS) val blocks: Double,
    @JsonProperty(STEALS) val steals: Double,
    @JsonProperty(TURNOVER) val turnovers: Double,
    @JsonProperty(PERSONAL_FOULS) val personalFouls: Double,
    @JsonProperty(FIELD_GOALS_ATTEMPTED) val fieldGoalsAttempted: Double,
    @JsonProperty(FIELD_GOALS_MADE) val fieldGoalsMade: Double,
    @JsonProperty(FIELD_GOAL_PERCENTAGE) val fieldGoalPercentage: Double,
    @JsonProperty(THREE_POINTERS_ATTEMPTED) val threePointersAttempted: Double,
    @JsonProperty(THREE_POINTERS_MADE) val threePointersMade: Double,
    @JsonProperty(THREE_POINTER_PERCENTAGE) val threePointerPercentage: Double,
    @JsonProperty(FREE_THROWS_ATTEMPTED) val freeThrowsAttempted: Double,
    @JsonProperty(FREE_THROWS_MADE) val freeThrowsMade: Double,
    @JsonProperty(FREE_THROW_PERCENTAGE) val freeThrowPercentage: Double,
)
