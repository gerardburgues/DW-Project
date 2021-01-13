package pl.pwr.nbaproject.model.amqp

data class SeasonAverageMessage(
    val playerIds: List<Int>,
    val season: Int? = null,
)
