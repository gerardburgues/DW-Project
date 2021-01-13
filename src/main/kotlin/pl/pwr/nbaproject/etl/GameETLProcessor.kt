package pl.pwr.nbaproject.etl

import com.fasterxml.jackson.databind.ObjectMapper
import org.springframework.r2dbc.core.DatabaseClient
import org.springframework.stereotype.Service
import pl.pwr.nbaproject.api.GamesClient
import pl.pwr.nbaproject.model.Queue
import pl.pwr.nbaproject.model.amqp.GameMessage
import pl.pwr.nbaproject.model.api.GamesWrapper
import pl.pwr.nbaproject.model.db.Game
import reactor.rabbitmq.Receiver
import kotlin.reflect.KClass

@Service
class GameETLProcessor(
    rabbitReceiver: Receiver,
    objectMapper: ObjectMapper,
    databaseClient: DatabaseClient,
    private val gamesClient: GamesClient,
) : AbstractETLProcessor<GameMessage, GamesWrapper, List<Game>>(rabbitReceiver, objectMapper, databaseClient) {

    override val queue = Queue.GAMES

    override val messageClass: KClass<GameMessage> = GameMessage::class

    override suspend fun extract(apiParams: GameMessage): GamesWrapper = with(apiParams) {
        gamesClient.getGames(seasons, teamIds, postSeason, page, perPage)
    }

    override suspend fun transform(data: GamesWrapper): List<Game> = data.data.map { game ->
        with(game) {
            Game(
                id, date, homeTeamScore,
                visitorTeamScore, season,
                period, status, time,
                postseason, homeTeam.id, visitorTeam.id,

                )

        }
    }

    override suspend fun load(data: List<Game>): List<String> = data.map { Game ->
        with(Game) {
            //language=Greenplum
            """
            |INSERT INTO games
            |(
            |    id,
            |    date,
            |    home_team_score,
            |    season,
            |    period,
            |    status,
            |    "time",
            |    postseason,
            |    home_team_id,
            |    visitor_team_id  
            |)
            |VALUES
            |(
            |    $id,
            |    $date,
            |    $homeTeamScore,
            |    $visitorTeamScore,
            |    $season,
            |    $period,
            |    $status,
            |    $time,
            |    $postseason,
            |    $homeTeamId,
            |    $visitorTeamId
            |)""".trimMargin()
        }
    }

}
