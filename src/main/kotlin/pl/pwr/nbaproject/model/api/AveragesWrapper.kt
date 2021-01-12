package pl.pwr.nbaproject.model.api

import com.fasterxml.jackson.annotation.JsonProperty

data class AveragesWrapper(
    @JsonProperty(DATA) val data: List<Average>
)
