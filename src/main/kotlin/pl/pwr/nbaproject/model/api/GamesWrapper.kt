package pl.pwr.nbaproject.model.api

import com.fasterxml.jackson.annotation.JsonProperty

data class GamesWrapper(
    @JsonProperty(DATA) val data: List<Game>,
    @JsonProperty(META) val meta: Meta
)
