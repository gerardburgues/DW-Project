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

@Service
class TeamsETLProcessor(
    rabbitReceiver: Receiver,
    objectMapper: ObjectMapper,
    databaseClient: DatabaseClient,
    private val teamsClient: TeamsClient,
) : AbstractETLProcessor<PageMessage, TeamsWrapper, List<Team>>(rabbitReceiver, objectMapper, databaseClient) {

    override val queue = TEAMS

    override val messageClass: Class<PageMessage> = PageMessage::class.java

    override suspend fun extract(apiParams: PageMessage): TeamsWrapper {
        return teamsClient.getTeams(apiParams.page)
    }

    override suspend fun transform(data: TeamsWrapper): List<Team> = data.data.map { team ->
        with(team) {
            Team(id, abbreviation, city, Conference.valueOf(conference), Division.valueOf(division), fullName, name)
        }
    }

    override suspend fun load(data: List<Team>): List<String> = data.map { team ->
        with(team) {
            //language=Greenplum
            """
            |INSERT INTO teams (id, abbreviation, city, conference, division, full_name, "name")
            |VALUES ($id, $abbreviation, $city, ${conference.name}, ${division.name}, $fullName, $name)
            |""".trimMargin()
        }
    }

}
