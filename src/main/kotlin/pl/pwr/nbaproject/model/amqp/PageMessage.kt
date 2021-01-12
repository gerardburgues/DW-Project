package pl.pwr.nbaproject.model.amqp

data class PageMessage(
    val page: Int = 0,
    val perPage: Int = 100,
)
