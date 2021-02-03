package pl.pwr.nbaproject.etl

import com.fasterxml.jackson.databind.ObjectMapper
import org.springframework.data.r2dbc.core.R2dbcEntityTemplate
import org.springframework.data.r2dbc.core.insert
import org.springframework.data.r2dbc.core.select
import org.springframework.data.relational.core.query.Criteria
import org.springframework.data.relational.core.query.Query
import org.springframework.data.relational.core.query.isEqual
import org.springframework.stereotype.Service
import pl.pwr.nbaproject.api.StatsClient
import pl.pwr.nbaproject.model.Queue
import pl.pwr.nbaproject.model.Queue.STATS
import pl.pwr.nbaproject.model.amqp.StatsMessage
import pl.pwr.nbaproject.model.api.StatsWrapper
import pl.pwr.nbaproject.model.db.Game
import pl.pwr.nbaproject.model.db.STATS_TABLE
import pl.pwr.nbaproject.model.db.Stats
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import reactor.kotlin.core.publisher.toFlux
import reactor.kotlin.core.publisher.toMono
import reactor.kotlin.extra.bool.not
import reactor.rabbitmq.Receiver
import reactor.rabbitmq.Sender
import kotlin.reflect.KClass

@Service
class StatsETLProcessor(
    rabbitReceiver: Receiver,
    rabbitSender: Sender,
    objectMapper: ObjectMapper,
    r2dbcEntityTemplate: R2dbcEntityTemplate,
    private val statsClient: StatsClient,
) : AbstractETLProcessor<StatsMessage, StatsWrapper, Stats>(
    rabbitReceiver,
    rabbitSender,
    objectMapper,
    r2dbcEntityTemplate,
) {

    override val queue: Queue = STATS

    override val tableName: String = STATS_TABLE

    override val messageClass: KClass<StatsMessage> = StatsMessage::class

    override fun extract(message: StatsMessage): Mono<StatsWrapper> = with(message) {
        statsClient.getStats(
            seasons = seasons,
            teamIds = teamIds,
            gameIds = gameIds,
            postSeason = postSeason,
            page = page,
            perPage = perPage
        )
    }

    override fun transform(data: StatsWrapper): Mono<Pair<List<Stats>, Boolean>> {
        val sendMessages = if (data.meta.currentPage == 1) {
            Flux.range(1, data.meta.totalPages)
                .flatMap { page ->
                    StatsMessage(
                        page = page + 1,
                        seasons = listOf(data.data.first().game.season)
                    ).toMono()
                }
                .`as` { messages -> sendMessages(messages) }
                .then(data.toMono())
        } else {
            data.toMono()
        }

        return sendMessages.map {
            val isLastPage = it.meta.currentPage == it.meta.totalPages
            val stats = it.data.map { stats ->
                with(stats) {
                    Stats(
                        id = id,
                        playerId = player.id,
                        teamId = team.id,
                        gameId = game.id,
                        homeTeamId = game.homeTeamId,
                        homeTeamScore = game.homeTeamScore,
                        visitorTeamId = game.visitorTeamId,
                        visitorTeamScore = game.visitorTeamScore,
                        winnerTeamId = if (game.homeTeamScore > game.visitorTeamScore) game.homeTeamId else game.visitorTeamId,
                        season = game.season,
                        date = game.date,
                        firstName = player.firstName,
                        lastName = player.lastName,
                        minutes = minutes,
                        points = points,
                        assists = assists,
                        rebounds = rebounds,
                        defensiveRebounds = defensiveRebounds,
                        offensiveRebounds = offensiveRebounds,
                        blocks = blocks,
                        steals = steals,
                        turnovers = turnovers,
                        personalFouls = personalFouls,
                        fieldGoalsAttempted = fieldGoalsAttempted,
                        fieldGoalsMade = fieldGoalsMade,
                        fieldGoalPercentage = fieldGoalPercentage,
                        threePointersAttempted = threePointersAttempted,
                        threePointersMade = threePointersMade,
                        threePointerPercentage = threePointerPercentage,
                        freeThrowsAttempted = freeThrowsAttempted,
                        freeThrowsMade = freeThrowsMade,
                        freeThrowPercentage = freeThrowPercentage,
                    )
                }
            }

            stats to isLastPage
        }
    }

    override fun load(data: Pair<List<Stats>, Boolean>): Mono<Boolean> = Flux.fromIterable(data.first)
        .filterWhen { stats ->
            r2dbcEntityTemplate.select<Game>()
                .matching(Query.query(Criteria.where("id").isEqual(stats.id)))
                .exists()
                .not()
        }
        .flatMap { team ->
            r2dbcEntityTemplate.insert<Stats>().using(team).onErrorContinue { e, _ -> }
        }
        .then(data.second.toMono())

    override fun prepareInitialMessages(): Flux<StatsMessage> = (2015..2021).toFlux()
        .map { season ->
            StatsMessage(page = 1, seasons = listOf(season))
        }
}
