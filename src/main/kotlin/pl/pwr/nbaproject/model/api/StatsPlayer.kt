package pl.pwr.nbaproject.model.api

import com.fasterxml.jackson.annotation.JsonProperty

data class StatsPlayer(
    @JsonProperty(ID) val id: Long,
    @JsonProperty(FIRST_NAME) val firstName: String,
    @JsonProperty(LAST_NAME) val lastName: String,
    @JsonProperty(POSITION) val position: String,
    @JsonProperty(HEIGHT_FEET) val heightFeet: Int?,
    @JsonProperty(HEIGHT_INCHES) val heightInches: Int?,
    @JsonProperty(WEIGHT_POUNDS) val weightPounds: Int?,
    @JsonProperty(TEAM_ID) val teamId: Long,
)
