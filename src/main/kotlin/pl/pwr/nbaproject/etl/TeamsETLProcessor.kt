package pl.pwr.nbaproject.etl

import com.fasterxml.jackson.databind.ObjectMapper
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.reactive.asFlow
import kotlinx.coroutines.reactive.awaitSingle
import org.springframework.r2dbc.core.DatabaseClient
import org.springframework.stereotype.Service
import pl.pwr.nbaproject.api.TeamsClient
import pl.pwr.nbaproject.model.Queue
import pl.pwr.nbaproject.model.amqp.YearMessage
import pl.pwr.nbaproject.model.api.Teams
import pl.pwr.nbaproject.model.db.TeamEntity
import reactor.rabbitmq.Receiver

@Service
class TeamsETLProcessor(
    rabbitReceiver: Receiver,
    objectMapper: ObjectMapper,
    private val teamsClient: TeamsClient,
    private val databaseClient: DatabaseClient,
) : AbstractETLProcessor<YearMessage, Teams, List<TeamEntity>>(rabbitReceiver, objectMapper) {

    override val queue: Queue = Queue.TEAMS

    override val messageClass: Class<YearMessage> = YearMessage::class.java

    override suspend fun extract(apiParams: YearMessage): Teams {
        return teamsClient.getTeams(apiParams)
    }

    override suspend fun transform(data: Teams): List<TeamEntity> {
        return data.league["standard"]?.map {
            TeamEntity(teamId = it.teamId)
        } ?: emptyList()
    }

    override suspend fun load(data: List<TeamEntity>) {
        val connection = databaseClient.connectionFactory.create().awaitSingle()
        val batch = connection.createBatch()
        data.forEach {
            batch.add("INSERT INTO teams (team_id) VALUES (${it.teamId}) ON CONFLICT DO NOTHING")
        }
        batch.execute().asFlow().collect()
    }

}
