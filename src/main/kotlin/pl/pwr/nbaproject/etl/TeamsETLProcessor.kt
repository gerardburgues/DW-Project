package pl.pwr.nbaproject.etl

import com.fasterxml.jackson.databind.ObjectMapper
import org.springframework.r2dbc.core.DatabaseClient
import org.springframework.stereotype.Service
import pl.pwr.nbaproject.api.TeamsClient
import pl.pwr.nbaproject.model.Queue.TEAMS
import pl.pwr.nbaproject.model.amqp.PageMessage
import pl.pwr.nbaproject.model.api.TeamsWrapper
import pl.pwr.nbaproject.model.db.Conference
import pl.pwr.nbaproject.model.db.Division
import pl.pwr.nbaproject.model.db.Team
import reactor.rabbitmq.Receiver
import reactor.rabbitmq.Sender
import kotlin.reflect.KClass

@Service
class TeamsETLProcessor(
    rabbitReceiver: Receiver,
    rabbitSender: Sender,
    objectMapper: ObjectMapper,
    databaseClient: DatabaseClient,
    private val teamsClient: TeamsClient,
) : AbstractETLProcessor<PageMessage, TeamsWrapper, List<Team>>(
    rabbitReceiver,
    rabbitSender,
    objectMapper,
    databaseClient,
) {

    override val queue = TEAMS

    override val messageClass: KClass<PageMessage> = PageMessage::class

    override suspend fun extract(apiParams: PageMessage): TeamsWrapper = with(apiParams) {
        teamsClient.getTeams(page, perPage)
    }

    override suspend fun transform(data: TeamsWrapper): List<Team> = data.data.map { team ->
        with(team) {
            Team(
                id = id,
                abbreviation = abbreviation,
                city = city,
                conference = Conference.valueOf(conference.toUpperCase()),
                division = Division.valueOf(division.toUpperCase()),
                fullName = fullName,
                name = name
            )
        }
    }

    override suspend fun load(data: List<Team>): List<String> = data.map { team ->
        with(team) {
            //language=Greenplum
            """
INSERT INTO teams (
    id,
    abbreviation,
    city,
    conference,
    division,
    full_name,
    name
) SELECT
    $id,
    '$abbreviation',
    '$city',
    '${conference.name}',
    '${division.name}',
    '$fullName',
    '$name'
WHERE NOT EXISTS (SELECT 1 FROM teams WHERE id = $id);"""
        }
    }

}
