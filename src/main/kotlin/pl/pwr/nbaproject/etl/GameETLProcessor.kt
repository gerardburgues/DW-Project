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
import pl.pwr.nbaproject.api.GamesClient
import pl.pwr.nbaproject.model.Queue.GAMES
import pl.pwr.nbaproject.model.amqp.GameMessage
import pl.pwr.nbaproject.model.api.GamesWrapper
import pl.pwr.nbaproject.model.db.GAMES_TABLE
import pl.pwr.nbaproject.model.db.Game
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

    override suspend fun extract(message: GameMessage): GamesWrapper = with(message) {
        gamesClient.getGames(seasons, teamIds, postSeason, page, perPage)
    }

    override suspend fun transform(data: GamesWrapper): Pair<List<Game>, Boolean> {
        if (data.meta.currentPage == 1) {
            for (i in 1 until data.meta.totalPages) {
                sendMessage(GameMessage(page = i + 1))
            }
        }

        return data.data.map { game ->
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
        } to (data.meta.currentPage == data.meta.totalPages)
    }

    override suspend fun load(data: Pair<List<Game>, Boolean>): Boolean {
        data.first
            .filterNot { game ->
                r2dbcEntityTemplate.select<Game>()
                    .matching(query(where("id").isEqual(game.id)))
                    .exists()
                    .awaitSingle()
            }
            .map { game ->
                r2dbcEntityTemplate.insert<Game>().usingAndAwait(game)
            }

        return data.second
    }

}
