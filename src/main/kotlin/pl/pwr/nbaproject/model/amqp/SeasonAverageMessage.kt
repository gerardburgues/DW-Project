package pl.pwr.nbaproject.model.amqp

data class SeasonAverageMessage(
    val playerIds: List<Long>,
    val season: Int? = null,
)
