package pl.pwr.nbaproject.model.api

import com.fasterxml.jackson.annotation.JsonProperty

data class Stats(
    @JsonProperty(ID) val id: Long,
    @JsonProperty(PLAYER) val player: StatsPlayer,
    @JsonProperty(TEAM) val team: Team,
    @JsonProperty(GAME) val game: StatsGame,
    @JsonProperty(MINUTES) val minutes: String,
    @JsonProperty(POINTS) val points: Int,
    @JsonProperty(ASSISTS) val assists: Int,
    @JsonProperty(REBOUNDS) val rebounds: Int,
    @JsonProperty(DEFENSIVE_REBOUNDS) val defensiveRebounds: Int,
    @JsonProperty(OFFENSIVE_REBOUNDS) val offensiveRebounds: Int,
    @JsonProperty(BLOCKS) val blocks: Int,
    @JsonProperty(STEALS) val steals: Int,
    @JsonProperty(TURNOVER) val turnovers: Int,
    @JsonProperty(PERSONAL_FOULS) val personalFouls: Int,
    @JsonProperty(FIELD_GOALS_ATTEMPTED) val fieldGoalsAttempted: Int,
    @JsonProperty(FIELD_GOALS_MADE) val fieldGoalsMade: Int,
    @JsonProperty(FIELD_GOAL_PERCENTAGE) val fieldGoalPercentage: Double,
    @JsonProperty(THREE_POINTERS_ATTEMPTED) val threePointersAttempted: Int,
    @JsonProperty(THREE_POINTERS_MADE) val threePointersMade: Int,
    @JsonProperty(THREE_POINTER_PERCENTAGE) val threePointerPercentage: Double,
    @JsonProperty(FREE_THROWS_ATTEMPTED) val freeThrowsAttempted: Int,
    @JsonProperty(FREE_THROWS_MADE) val freeThrowsMade: Int,
    @JsonProperty(FREE_THROW_PERCENTAGE) val freeThrowPercentage: Double,
)
