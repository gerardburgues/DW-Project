package pl.pwr.nbaproject.etl

import com.fasterxml.jackson.databind.ObjectMapper
import org.reactivestreams.Publisher
import org.springframework.data.r2dbc.core.R2dbcEntityTemplate
import org.springframework.data.r2dbc.core.insert
import org.springframework.stereotype.Service
import pl.pwr.nbaproject.api.TeamsClient
import pl.pwr.nbaproject.model.Queue.TEAMS
import pl.pwr.nbaproject.model.amqp.PageMessage
import pl.pwr.nbaproject.model.api.TeamsWrapper
import pl.pwr.nbaproject.model.db.Conference
import pl.pwr.nbaproject.model.db.Division
import pl.pwr.nbaproject.model.db.TEAMS_TABLE
import pl.pwr.nbaproject.model.db.Team
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
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

    override fun extract(message: Mono<PageMessage>): Mono<TeamsWrapper> = message.flatMap {
        with(it) {
            teamsClient.getTeams(page, perPage)
        }
    }

    override fun transform(data: Mono<TeamsWrapper>): Mono<Pair<List<Team>, Boolean>> {
        return data.doOnNext { teamsWrapper ->
            if (teamsWrapper.meta.currentPage == 1) {
                Flux.fromIterable(1 until teamsWrapper.meta.totalPages)
                    .flatMap { page -> sendMessages(Mono.fromCallable { PageMessage(page = page + 1) }) }
                    .subscribe()
            }
        }.map { teamsWrapper ->
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

    override fun load(data: Mono<Pair<List<Team>, Boolean>>): Mono<Boolean> = data.flatMap { pair ->
        Flux.fromIterable(pair.first)
            .flatMap { team ->
                r2dbcEntityTemplate.insert<Team>().using(team)
            }
            .then(Mono.just(pair.second))
    }

    override fun prepareInitialMessages(): Publisher<PageMessage> = Mono.fromCallable { PageMessage() }

}
