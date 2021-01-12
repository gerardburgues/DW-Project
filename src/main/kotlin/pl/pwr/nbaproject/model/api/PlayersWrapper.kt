package pl.pwr.nbaproject.model.api

import com.fasterxml.jackson.annotation.JsonProperty

data class PlayersWrapper(
    @JsonProperty(DATA) val data: List<Player>,
    @JsonProperty(META) val meta: Meta
)
