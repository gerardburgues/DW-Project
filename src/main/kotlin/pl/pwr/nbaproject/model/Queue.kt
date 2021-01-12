package pl.pwr.nbaproject.model

enum class Queue(val queueName: String) {
    AVERAGES("averages"),
    GAMES("games"),
    PLAYERS("players"),
    STATS("stats"),
    TEAMS("teams");

    fun amqpQueue() = org.springframework.amqp.core.Queue(queueName)
}
