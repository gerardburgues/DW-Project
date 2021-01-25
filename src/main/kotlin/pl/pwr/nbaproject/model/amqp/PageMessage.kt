package pl.pwr.nbaproject.model.amqp

data class PageMessage(
    val page: Int = 1,
    val perPage: Int = 100,
)
