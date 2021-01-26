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
import pl.pwr.nbaproject.api.TeamsClient
import pl.pwr.nbaproject.model.Queue.TEAMS
import pl.pwr.nbaproject.model.amqp.PageMessage
import pl.pwr.nbaproject.model.api.TeamsWrapper
import pl.pwr.nbaproject.model.db.Conference
import pl.pwr.nbaproject.model.db.Division
import pl.pwr.nbaproject.model.db.TEAMS_TABLE
import pl.pwr.nbaproject.model.db.Team
import reactor.rabbitmq.Receiver
import reactor.rabbitmq.Sender
import kotlin.reflect.KClass

@Service
class TeamsETLProcessor(
    rabbitReceiver: Receiver,
    rabbitSender: Sender,
    objectMapper: ObjectMapper,
    r2dbcEntityTemplate: R2dbcEntityTemplate,
    private val teamsClient: TeamsClient,
) : AbstractETLProcessor<PageMessage, TeamsWrapper, Team>(
    rabbitReceiver,
    rabbitSender,
    objectMapper,
    r2dbcEntityTemplate,
) {

    override val queue = TEAMS

    override val tableName: String = TEAMS_TABLE

    override val messageClass: KClass<PageMessage> = PageMessage::class

    override suspend fun extract(message: PageMessage): TeamsWrapper = with(message) {
        teamsClient.getTeams(page, perPage)
    }

    override suspend fun transform(data: TeamsWrapper): Pair<List<Team>, Boolean> {
        if (data.meta.currentPage == 1) {
            for (i in 1 until data.meta.totalPages) {
                sendMessage(PageMessage(page = i + 1))
            }
        }

        return data.data.map { team ->
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
        } to (data.meta.currentPage == data.meta.totalPages)
    }

    override suspend fun load(data: Pair<List<Team>, Boolean>): Boolean {
        data.first
            .filterNot { team ->
                r2dbcEntityTemplate.select<Team>()
                    .matching(query(where("id").isEqual(team.id)))
                    .exists()
                    .awaitSingle()
            }
            .map { team ->
                r2dbcEntityTemplate.insert<Team>().usingAndAwait(team)
            }

        return data.second
    }

}
