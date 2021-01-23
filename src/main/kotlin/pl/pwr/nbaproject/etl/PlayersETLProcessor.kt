package pl.pwr.nbaproject.etl

import com.fasterxml.jackson.databind.ObjectMapper
import org.springframework.r2dbc.core.DatabaseClient
import org.springframework.stereotype.Service
import pl.pwr.nbaproject.api.PlayersClient
import pl.pwr.nbaproject.model.Queue
import pl.pwr.nbaproject.model.amqp.PageMessage
import pl.pwr.nbaproject.model.api.PlayersWrapper
import pl.pwr.nbaproject.model.db.Player
import reactor.rabbitmq.Receiver
import kotlin.reflect.KClass

@Service
class PlayersETLProcessor(
    rabbitReceiver: Receiver,
    objectMapper: ObjectMapper,
    databaseClient: DatabaseClient,
    private val playersClient: PlayersClient,
) : AbstractETLProcessor<PageMessage, PlayersWrapper, List<Player>>(rabbitReceiver, objectMapper, databaseClient) {

    override val queue = Queue.PLAYERS

    override val messageClass: KClass<PageMessage> = PageMessage::class

    override suspend fun extract(apiParams: PageMessage): PlayersWrapper = with(apiParams) {
        playersClient.getPlayers(page, perPage)
    }

    override suspend fun transform(data: PlayersWrapper): List<Player> = data.data.map { player ->
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

    override suspend fun load(data: List<Player>): List<String> = data.map { player ->
        with(player) {
            //language=Greenplum
            """
INSERT INTO players (
    id,
    first_name,
    last_name,
    position,
    height_feet,
    height_inches,
    weight_pounds,
    team_id
 ) VALUES (
    $id,
    $firstName,
    $lastName,
    $position,
    $heightFeet,
    $heightInches,
    $weightPounds,
    $teamId
)"""
        }
    }
}
