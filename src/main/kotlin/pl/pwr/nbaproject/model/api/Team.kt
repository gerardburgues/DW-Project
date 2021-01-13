package pl.pwr.nbaproject.model.api

import com.fasterxml.jackson.annotation.JsonProperty

data class Team(
    @JsonProperty(ID) val id: Long,
    @JsonProperty(ABBREVIATION) val abbreviation: String,
    @JsonProperty(CITY) val city: String,
    @JsonProperty(CONFERENCE) val conference: String,
    @JsonProperty(DIVISION) val division: String,
    @JsonProperty(FULL_NAME) val fullName: String,
    @JsonProperty(NAME) val name: String,
)
