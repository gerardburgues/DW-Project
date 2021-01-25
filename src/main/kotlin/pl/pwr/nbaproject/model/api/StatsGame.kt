package pl.pwr.nbaproject.model.api

import com.fasterxml.jackson.annotation.JsonProperty
import java.time.LocalDate

data class StatsGame(
    @JsonProperty(ID) val id: Long,
    @JsonProperty(DATE) val date: LocalDate,
    @JsonProperty(HOME_TEAM_SCORE) val homeTeamScore: Int,
    @JsonProperty(VISITOR_TEAM_SCORE) val visitorTeamScore: Int,
    @JsonProperty(SEASON) val season: Int,
    @JsonProperty(PERIOD) val period: Int?,
    @JsonProperty(STATUS) val status: String?,
    @JsonProperty(TIME) val time: String?,
    @JsonProperty(POSTSEASON) val postseason: Boolean?,
    @JsonProperty(HOME_TEAM_ID) val homeTeamId: Long,
    @JsonProperty(VISITOR_TEAM_ID) val visitorTeamId: Long,
)
