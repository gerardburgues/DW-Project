package pl.pwr.nbaproject.model.amqp

data class SeasonAverageMessage(
    val season: Int? = null,
    val playerIds: List<Int>
)
