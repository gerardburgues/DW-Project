package pl.pwr.nbaproject

import com.rabbitmq.client.ConnectionFactory
import org.springframework.amqp.core.AmqpAdmin
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.scheduling.annotation.EnableScheduling
import org.springframework.web.reactive.function.client.WebClient
import pl.pwr.nbaproject.model.Queue
import reactor.rabbitmq.*
import javax.annotation.PostConstruct

@Configuration
@EnableScheduling
class Context(
    private val amqpAdmin: AmqpAdmin
) {

    @Bean
    fun ballDontLieWebClient(): WebClient = WebClient.builder()
        .baseUrl("https://www.balldontlie.io/api/v1")
        .build()

    @Bean
    fun rabbitReactorConnectionFactory(
        @Value("\${spring.rabbitmq.host}") host: String,
        @Value("\${spring.rabbitmq.port}") port: Int,
        @Value("\${spring.rabbitmq.username}") username: String,
        @Value("\${spring.rabbitmq.password}") password: String,
    ) = ConnectionFactory().apply {
        this.host = host
        this.port = port
        this.username = username
        this.password = password
    }

    @Bean
    fun rabbitReceiver(rabbitReactorConnectionFactory: ConnectionFactory): Receiver = RabbitFlux.createReceiver(
        ReceiverOptions().connectionFactory(rabbitReactorConnectionFactory)
    )

    @Bean
    fun rabbitSender(rabbitReactorConnectionFactory: ConnectionFactory): Sender = RabbitFlux.createSender(
        SenderOptions().connectionFactory(rabbitReactorConnectionFactory)
    )

    @PostConstruct
    fun init() {
        Queue.values().forEach { queue ->
            amqpAdmin.declareQueue(queue.amqpQueue())
        }
    }

}
