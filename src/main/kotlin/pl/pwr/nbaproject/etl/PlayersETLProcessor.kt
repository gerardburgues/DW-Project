package pl.pwr.nbaproject.etl

import com.fasterxml.jackson.databind.ObjectMapper
import org.reactivestreams.Publisher
import org.springframework.data.r2dbc.core.R2dbcEntityTemplate
import org.springframework.data.r2dbc.core.insert
import org.springframework.stereotype.Service
import pl.pwr.nbaproject.api.PlayersClient
import pl.pwr.nbaproject.model.Queue.PLAYERS
import pl.pwr.nbaproject.model.amqp.PageMessage
import pl.pwr.nbaproject.model.api.PlayersWrapper
import pl.pwr.nbaproject.model.db.PLAYERS_TABLE
import pl.pwr.nbaproject.model.db.Player
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
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

    override fun extract(message: Mono<PageMessage>): Mono<PlayersWrapper> = message.flatMap {
        with(it) {
            playersClient.getPlayers(page, perPage)
        }
    }

    override fun transform(data: Mono<PlayersWrapper>): Mono<Pair<List<Player>, Boolean>> {
        return data.doOnNext { playersWrapper ->
            if (playersWrapper.meta.currentPage == 1) {
                Flux.fromIterable(1 until playersWrapper.meta.totalPages)
                    .flatMap { page -> sendMessages(Mono.fromCallable { PageMessage(page = page + 1) }) }
                    .subscribe()
            }
        }.map { playersWrapper ->
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

    override fun load(data: Mono<Pair<List<Player>, Boolean>>): Mono<Boolean> = data.flatMap { pair ->
        Flux.fromIterable(pair.first)
            .flatMap { player ->
                r2dbcEntityTemplate.insert<Player>().using(player)
            }
            .then(Mono.just(pair.second))
    }

    override fun prepareInitialMessages(): Publisher<PageMessage> = Mono.fromCallable { PageMessage() }

}
