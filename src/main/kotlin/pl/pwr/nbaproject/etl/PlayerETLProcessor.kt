package pl.pwr.nbaproject.etl

import com.fasterxml.jackson.databind.ObjectMapper
import org.springframework.stereotype.Service
import pl.pwr.nbaproject.api.PlayersClient
import pl.pwr.nbaproject.etl.dataholders.PlayersTL
import pl.pwr.nbaproject.model.Queue
import pl.pwr.nbaproject.model.amqp.PageMessage
import pl.pwr.nbaproject.model.api.playergame.Players
import reactor.rabbitmq.Receiver

@Service
class PlayerETLProcessor(
    rabbitReceiver: Receiver,
    objectMapper: ObjectMapper,
    private val playersClient: PlayersClient
) : AbstractETLProcessor<PageMessage, Players, PlayersTL>(rabbitReceiver, objectMapper) {
    override val queue: Queue = Queue.PLAYERS
    override val messageClass: Class<PageMessage> = PageMessage::class.java

    /*    override suspend fun extract(apiParams: YearMessage): Players {
            return playersClient.getPlayers(apiParams.year)
        }*/
    override suspend fun extract(apiParams: PageMessage): Players {
        return playersClient.getPlayers(apiParams.page)
    }

    override suspend fun transform(data: Players): PlayersTL = TODO()

    override suspend fun load(data: PlayersTL) {
        TODO("Not yet implemented")
    }


}