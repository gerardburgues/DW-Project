package pl.pwr.nbaproject.etl

import com.fasterxml.jackson.databind.ObjectMapper
import kotlinx.coroutines.reactive.awaitSingle
import org.springframework.data.r2dbc.core.R2dbcEntityTemplate
import org.springframework.data.r2dbc.core.insert
import org.springframework.data.r2dbc.core.select
import org.springframework.data.r2dbc.core.usingAndAwait
import org.springframework.data.relational.core.query.Criteria.where
import org.springframework.data.relational.core.query.Query.query
import org.springframework.data.relational.core.query.isEqual
import org.springframework.stereotype.Service
import pl.pwr.nbaproject.api.PlayersClient
import pl.pwr.nbaproject.model.Queue
import pl.pwr.nbaproject.model.amqp.PageMessage
import pl.pwr.nbaproject.model.api.PlayersWrapper
import pl.pwr.nbaproject.model.db.Player
import reactor.rabbitmq.Receiver
import reactor.rabbitmq.Sender
import kotlin.reflect.KClass

@Service
class PlayersETLProcessor(
    rabbitReceiver: Receiver,
    rabbitSender: Sender,
    objectMapper: ObjectMapper,
    r2dbcEntityTemplate: R2dbcEntityTemplate,
    private val playersClient: PlayersClient,
) : AbstractETLProcessor<PageMessage, PlayersWrapper, Player>(
    rabbitReceiver,
    rabbitSender,
    objectMapper,
    r2dbcEntityTemplate,
) {

    override val queue = Queue.PLAYERS

    override val messageClass: KClass<PageMessage> = PageMessage::class

    override suspend fun extract(apiParams: PageMessage): PlayersWrapper = with(apiParams) {
        playersClient.getPlayers(page, perPage)
    }

    override suspend fun transform(data: PlayersWrapper): List<Player> {
        if (data.meta.nextPage != null) {
            sendMessage(PageMessage(data.meta.nextPage))
        }

        return data.data.map { player ->
            with(player) {
                Player(
                    id = id,
                    firstName = firstName,
                    lastName = lastName,
                    position = position,
                    heightFeet = heightFeet,
                    heightInches = heightInches,
                    weightPounds = weightPounds,
                    teamId = team.id
                )
            }
        }
    }

    override suspend fun load(data: List<Player>) {
        data
            .filterNot { player ->
                r2dbcEntityTemplate.select<Player>()
                    .matching(query(where("id").isEqual(player.id)))
                    .exists()
                    .awaitSingle()
            }
            .map { player ->
                r2dbcEntityTemplate.insert<Player>().usingAndAwait(player)
            }
    }

}
