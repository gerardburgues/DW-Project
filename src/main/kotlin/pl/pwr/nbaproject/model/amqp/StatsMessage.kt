package pl.pwr.nbaproject.model.amqp

data class StatsMessage(
    val seasons: List<Int> = emptyList(),
    val teamIds: List<Int> = emptyList(),
    val gameIds: List<Int> = emptyList(),
    val postSeason: Boolean? = null,
    val page: Int = 0,
    val perPage: Int = 100,
)
