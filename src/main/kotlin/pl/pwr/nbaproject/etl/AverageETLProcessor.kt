package pl.pwr.nbaproject.etl

import com.fasterxml.jackson.databind.ObjectMapper
import org.springframework.r2dbc.core.DatabaseClient
import org.springframework.stereotype.Service
import pl.pwr.nbaproject.api.AverageClient
import pl.pwr.nbaproject.etl.dataholders.AverageTL
import pl.pwr.nbaproject.model.Queue
import pl.pwr.nbaproject.model.amqp.PageMessage
import pl.pwr.nbaproject.model.api.playeravg.AveragePlayer
import reactor.rabbitmq.Receiver

@Service
class AverageETLProcessor(
    rabbitReceiver: Receiver,
    objectMapper: ObjectMapper,
    private val averageClient: AverageClient,
    private val databaseClient: DatabaseClient,
) : AbstractETLProcessor<PageMessage, AveragePlayer, AverageTL>(rabbitReceiver, objectMapper) {

    override val queue: Queue = Queue.PLAYERS
    override val messageClass: Class<PageMessage> = PageMessage::class.java

    override suspend fun extract(apiParams: PageMessage): AveragePlayer {
        return averageClient.getAverage(apiParams.page)
    }

    override suspend fun transform(data: AveragePlayer): AverageTL {
        TODO("Not yet implemented")
    }

    override suspend fun load(data: AverageTL) {
        TODO("Not yet implemented")
    }

}
