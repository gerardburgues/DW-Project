package pl.pwr.nbaproject.model

enum class Queue(val queueName: String) {
    TEAMS("teams");

    fun amqpQueue() = org.springframework.amqp.core.Queue(queueName)
}
