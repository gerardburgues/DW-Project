package pl.pwr.nbaproject

import com.rabbitmq.client.ConnectionFactory
import org.springframework.amqp.core.AmqpAdmin
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.http.HttpHeaders
import org.springframework.http.HttpHeaders.*
import org.springframework.http.MediaType
import org.springframework.scheduling.annotation.EnableScheduling
import org.springframework.web.reactive.function.client.WebClient
import pl.pwr.nbaproject.model.Queue
import reactor.rabbitmq.*
import java.util.*
import java.util.function.Consumer
import javax.annotation.PostConstruct

@Configuration
@EnableScheduling
class Context(
    private val amqpAdmin: AmqpAdmin
) {

    companion object {
        private const val X_NBA_STATS_ORIGIN = "x-nba-stats-origin"
        private const val X_NBA_STATS_TOKEN = "x-nba-stats-token"
    }

    @Bean
    fun nbaHeaders(): Consumer<HttpHeaders> = Consumer {
        it.apply {
            accept = listOf(MediaType.APPLICATION_JSON, MediaType.TEXT_PLAIN, MediaType.ALL)
            acceptLanguageAsLocales = listOf(Locale.US, Locale.ENGLISH)
            cacheControl = "no-cache"
            connection = listOf("keep-alive")
            pragma = "no-cache"

            put(ACCEPT_ENCODING, listOf("gzip, deflate, br"))
            put(
                USER_AGENT,
                listOf("Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:72.0) Gecko/20100101 Firefox/72.0")
            )
            put(X_NBA_STATS_ORIGIN, listOf("stats"))
            put(X_NBA_STATS_TOKEN, listOf("true"))
        }
    }

    @Bean
    fun dataWebClient(nbaHeaders: Consumer<HttpHeaders>): WebClient = WebClient.builder()
        .defaultHeaders(nbaHeaders)
        .defaultHeader(HOST, "data.nba.com")
        .defaultHeader(REFERER, "http://data.nba.com/")
        .baseUrl("http://data.nba.com/")
        .build()

    @Bean
    fun statsWebClient(nbaHeaders: Consumer<HttpHeaders>): WebClient = WebClient.builder()
        .defaultHeaders(nbaHeaders)
        .defaultHeader(HOST, "stats.nba.com")
        .defaultHeader(REFERER, "http://stats.nba.com/")
        .baseUrl("http://stats.nba.com/stats")
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
