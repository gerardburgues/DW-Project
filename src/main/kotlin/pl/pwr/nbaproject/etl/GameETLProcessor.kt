package pl.pwr.nbaproject.etl

import com.fasterxml.jackson.databind.ObjectMapper
import org.springframework.data.r2dbc.core.R2dbcEntityTemplate
import org.springframework.data.r2dbc.core.insert
import org.springframework.stereotype.Service
import pl.pwr.nbaproject.api.GamesClient
import pl.pwr.nbaproject.model.Queue.GAMES
import pl.pwr.nbaproject.model.amqp.GameMessage
import pl.pwr.nbaproject.model.api.GamesWrapper
import pl.pwr.nbaproject.model.db.GAMES_TABLE
import pl.pwr.nbaproject.model.db.Game
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import reactor.kotlin.core.publisher.toFlux
import reactor.rabbitmq.Receiver
import reactor.rabbitmq.Sender
import kotlin.reflect.KClass

@Service
class GameETLProcessor(
    rabbitReceiver: Receiver,
    rabbitSender: Sender,
    objectMapper: ObjectMapper,
    r2dbcEntityTemplate: R2dbcEntityTemplate,
    private val gamesClient: GamesClient,
) : AbstractETLProcessor<GameMessage, GamesWrapper, Game>(
    rabbitReceiver,
    rabbitSender,
    objectMapper,
    r2dbcEntityTemplate,
) {

    override val queue = GAMES

    override val tableName: String = GAMES_TABLE

    override val messageClass: KClass<GameMessage> = GameMessage::class

    override fun extract(message: Mono<GameMessage>): Mono<GamesWrapper> = message.flatMap {
        with(it) {
            gamesClient.getGames(seasons, teamIds, postSeason, page, perPage)
        }
    }

    override fun transform(data: Mono<GamesWrapper>): Mono<Pair<List<Game>, Boolean>> {
        return data.doOnNext { gameWrapper ->
            if (gameWrapper.meta.currentPage == 1) {
                Flux.fromIterable(1 until gameWrapper.meta.totalPages)
                    .flatMap { page -> sendMessages(Mono.fromCallable { GameMessage(page = page + 1) }) }
                    .subscribe()
            }
        }.map { gameWrapper ->
            val isLastPage = gameWrapper.meta.currentPage == gameWrapper.meta.totalPages
            val games = gameWrapper.data.map { game ->
                with(game) {
                    Game(
                        id = id,
                        date = date,
                        homeTeamScore = homeTeamScore,
                        visitorTeamScore = visitorTeamScore,
                        season = season,
                        period = period,
                        status = status,
                        time = time,
                        postseason = postseason,
                        homeTeamId = homeTeam.id,
                        visitorTeamId = visitorTeam.id,
                        winnerTeamId = if (homeTeamScore > visitorTeamScore) homeTeam.id else visitorTeam.id
                    )
                }
            }

            games to isLastPage
        }
    }

    override fun load(data: Mono<Pair<List<Game>, Boolean>>): Mono<Boolean> = data.flatMap { pair ->
        Flux.fromIterable(pair.first)
            .flatMap { game ->
                r2dbcEntityTemplate.insert<Game>().using(game)
            }
            .then(Mono.just(pair.second))
    }

    override fun prepareInitialMessages(): Flux<GameMessage> = (2015..2021).toFlux()
        .map { season ->
            GameMessage(seasons = listOf(season))
        }
}
