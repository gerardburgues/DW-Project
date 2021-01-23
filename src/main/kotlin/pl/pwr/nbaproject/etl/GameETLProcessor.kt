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
import reactor.rabbitmq.Sender
import kotlin.reflect.KClass

@Service
class GameETLProcessor(
    rabbitReceiver: Receiver,
    rabbitSender: Sender,
    objectMapper: ObjectMapper,
    databaseClient: DatabaseClient,
    private val gamesClient: GamesClient,
) : AbstractETLProcessor<GameMessage, GamesWrapper, List<Game>>(
    rabbitReceiver,
    rabbitSender,
    objectMapper,
    databaseClient,
) {

    override val queue = Queue.GAMES

    override val messageClass: KClass<GameMessage> = GameMessage::class

    override suspend fun extract(apiParams: GameMessage): GamesWrapper = with(apiParams) {
        gamesClient.getGames(seasons, teamIds, postSeason, page, perPage)
    }

    override suspend fun transform(data: GamesWrapper): List<Game> = data.data.map { game ->
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

    override suspend fun load(data: List<Game>): List<String> = data.map { Game ->
        with(Game) {
            //language=Greenplum
            """
INSERT INTO games (
    id,
    date,
    home_team_score,
    visitor_team_score,
    season,
    period,
    status,
    time,
    postseason,
    home_team_id,
    visitor_team_id,
    winner_team_id
) SELECT 
    $id,
    '$date',
    $homeTeamScore,
    $visitorTeamScore,
    $season,
    $period,
    '$status',
    '$time',
    $postseason,
    $homeTeamId,
    $visitorTeamId,
    $winnerTeamId
WHERE NOT EXISTS (SELECT 1 FROM games WHERE id = $id);"""
        }
    }

}
