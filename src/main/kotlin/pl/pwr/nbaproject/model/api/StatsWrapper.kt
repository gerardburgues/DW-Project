package pl.pwr.nbaproject.model.api

import com.fasterxml.jackson.annotation.JsonProperty

data class StatsWrapper(
    @JsonProperty(DATA) val data: List<Stats>,
    @JsonProperty(META) val meta: Meta
)
