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

    override val messageClass: KClass<PageMessage> = PageMessage::class

    override suspend fun extract(apiParams: PageMessage): TeamsWrapper = with(apiParams) {
        teamsClient.getTeams(page, perPage)
    }

    override suspend fun transform(data: TeamsWrapper): List<Team> {
        if (data.meta.nextPage != null) {
            sendMessage(PageMessage(page = data.meta.nextPage))
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
        }
    }

    override suspend fun load(data: List<Team>) {
        data
            .filterNot { team ->
                r2dbcEntityTemplate.select<Team>()
                    .matching(query(where("id").isEqual(team.id)))
                    .exists()
                    .awaitSingle()
            }
            .map { team ->
                r2dbcEntityTemplate.insert<Team>().usingAndAwait(team)
            }
    }

}
