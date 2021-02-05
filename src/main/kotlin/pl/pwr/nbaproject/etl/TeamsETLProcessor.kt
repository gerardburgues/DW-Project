package pl.pwr.nbaproject.etl

import com.fasterxml.jackson.databind.ObjectMapper
import org.reactivestreams.Publisher
import org.springframework.data.r2dbc.core.R2dbcEntityTemplate
import org.springframework.data.r2dbc.core.insert
import org.springframework.data.r2dbc.core.select
import org.springframework.data.relational.core.query.Criteria
import org.springframework.data.relational.core.query.Query
import org.springframework.data.relational.core.query.isEqual
import org.springframework.stereotype.Service
import pl.pwr.nbaproject.api.TeamsClient
import pl.pwr.nbaproject.model.Queue.TEAMS
import pl.pwr.nbaproject.model.amqp.PageMessage
import pl.pwr.nbaproject.model.api.TeamsWrapper
import pl.pwr.nbaproject.model.db.*
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import reactor.kotlin.core.publisher.toMono
import reactor.kotlin.extra.bool.not
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

    override fun extract(message: PageMessage): Mono<TeamsWrapper> = with(message) {
        teamsClient.getTeams(page, perPage)
    }

    override fun transform(data: TeamsWrapper): Mono<Pair<List<Team>, Boolean>> {
        val sendMessages = if (data.meta.currentPage == 1) {
            Flux.fromIterable(1 until data.meta.totalPages)
                .flatMap { page -> sendMessages(PageMessage(page = page + 1).toMono()) }
                .then(data.toMono())
        } else {
            data.toMono()
        }

        return sendMessages.map { teamsWrapper ->
            val isLastPage = teamsWrapper.meta.currentPage == teamsWrapper.meta.totalPages
            val teams = teamsWrapper.data.map { team ->
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

            teams to isLastPage
        }
    }

    override fun load(data: Pair<List<Team>, Boolean>): Mono<Boolean> = Flux.fromIterable(data.first)
        .filterWhen { team ->
            r2dbcEntityTemplate.select<Game>()
                .matching(Query.query(Criteria.where("id").isEqual(team.id)))
                .exists()
                .not()
        }
        .flatMap { team ->
            r2dbcEntityTemplate.insert<Team>().using(team).onErrorContinue { e, _ -> }
        }
        .then(data.second.toMono())


    override fun prepareInitialMessages(): Publisher<PageMessage> = PageMessage().toMono()

}
