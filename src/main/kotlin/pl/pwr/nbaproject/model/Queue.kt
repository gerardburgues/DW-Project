package pl.pwr.nbaproject.model

enum class Queue(val queueName: String) {
    TEAMS("teams"),
    PLAYERS("players");

    fun amqpQueue() = org.springframework.amqp.core.Queue(queueName)
}
