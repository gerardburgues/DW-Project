package pl.pwr.nbaproject.etl

import com.fasterxml.jackson.databind.ObjectMapper
import org.reactivestreams.Publisher
import org.springframework.data.r2dbc.core.R2dbcEntityTemplate
import org.springframework.data.r2dbc.core.insert
import org.springframework.data.r2dbc.core.select
import org.springframework.data.relational.core.query.Criteria
import org.springframework.data.relational.core.query.Query
import org.springframework.data.relational.core.query.isEqual
import org.springframework.stereotype.Service
import pl.pwr.nbaproject.api.PlayersClient
import pl.pwr.nbaproject.model.Queue.PLAYERS
import pl.pwr.nbaproject.model.amqp.PageMessage
import pl.pwr.nbaproject.model.api.PlayersWrapper
import pl.pwr.nbaproject.model.db.Game
import pl.pwr.nbaproject.model.db.PLAYERS_TABLE
import pl.pwr.nbaproject.model.db.Player
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import reactor.kotlin.core.publisher.toMono
import reactor.kotlin.extra.bool.not
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

    override val queue = PLAYERS

    override val tableName: String = PLAYERS_TABLE

    override val messageClass: KClass<PageMessage> = PageMessage::class

    override fun extract(message: PageMessage): Mono<PlayersWrapper> = with(message) {
        playersClient.getPlayers(page, perPage)
    }

    override fun transform(data: PlayersWrapper): Mono<Pair<List<Player>, Boolean>> {
        val sendMessages = if (data.meta.currentPage == 1) {
            Flux.fromIterable(1 until data.meta.totalPages)
                .flatMap { page -> sendMessages(PageMessage(page = page + 1).toMono()) }
                .then(data.toMono())
        } else {
            data.toMono()
        }

        return sendMessages.map { playersWrapper ->
            val isLastPage = playersWrapper.meta.currentPage == playersWrapper.meta.totalPages
            val players = playersWrapper.data.map { player ->
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

            players to isLastPage
        }
    }

    override fun load(data: Pair<List<Player>, Boolean>): Mono<Boolean> = Flux.fromIterable(data.first)
        .filterWhen { player ->
            r2dbcEntityTemplate.select<Game>()
                .matching(Query.query(Criteria.where("id").isEqual(player.id)))
                .exists()
                .not()
        }
        .flatMap { player ->
            r2dbcEntityTemplate.insert<Player>().using(player).onErrorContinue { e, _ -> }
        }
        .then(data.second.toMono())

    override fun prepareInitialMessages(): Publisher<PageMessage> = PageMessage().toMono()

}
