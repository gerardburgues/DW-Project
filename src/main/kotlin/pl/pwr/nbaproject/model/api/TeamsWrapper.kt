package pl.pwr.nbaproject.model.api

import com.fasterxml.jackson.annotation.JsonProperty

data class TeamsWrapper(
    @JsonProperty(DATA) val data: List<Team>,
    @JsonProperty(META) val meta: Meta
)
