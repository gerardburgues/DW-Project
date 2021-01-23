package pl.pwr.nbaproject.model.api

import com.fasterxml.jackson.annotation.JsonProperty

data class Meta(
    @JsonProperty(TOTAL_PAGES) val totalPages: Int,
    @JsonProperty(CURRENT_PAGE) val currentPage: Int,
    @JsonProperty(NEXT_PAGE) val nextPage: Int?,
    @JsonProperty(PER_PAGE) val perPage: Int,
    @JsonProperty(TOTAL_COUNT) val totalCount: Int,
)
