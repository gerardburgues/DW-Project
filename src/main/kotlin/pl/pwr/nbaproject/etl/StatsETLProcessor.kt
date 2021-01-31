package pl.pwr.nbaproject.etl

import com.fasterxml.jackson.databind.ObjectMapper
import org.springframework.data.r2dbc.core.R2dbcEntityTemplate
import org.springframework.data.r2dbc.core.insert
import org.springframework.stereotype.Service
import pl.pwr.nbaproject.api.StatsClient
import pl.pwr.nbaproject.model.Queue
import pl.pwr.nbaproject.model.Queue.STATS
import pl.pwr.nbaproject.model.amqp.StatsMessage
import pl.pwr.nbaproject.model.api.StatsWrapper
import pl.pwr.nbaproject.model.db.STATS_TABLE
import pl.pwr.nbaproject.model.db.Stats
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import reactor.kotlin.core.publisher.toFlux
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

    override fun extract(message: Mono<StatsMessage>): Mono<StatsWrapper> = message.flatMap {
        with(it) {
            statsClient.getStats(
                seasons,
                teamIds,
                gameIds,
                postSeason,
                page,
                page
            )
        }
    }

    override fun transform(data: Mono<StatsWrapper>): Mono<Pair<List<Stats>, Boolean>> {
        return data.doOnNext { statsWrapper ->
            if (statsWrapper.meta.currentPage == 1) {
                Flux.fromIterable(1 until statsWrapper.meta.totalPages)
                    .flatMap { i ->
                        sendMessages(Mono.fromCallable {
                            StatsMessage(
                                page = i + 1,
                                seasons = listOf(statsWrapper.data.first().game.season)
                            )
                        })
                    }
                    .subscribe()
            }
        }.map {
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

    override fun load(data: Mono<Pair<List<Stats>, Boolean>>): Mono<Boolean> = data.flatMap { pair ->
        Flux.fromIterable(pair.first)
            .flatMap { team ->
                r2dbcEntityTemplate.insert<Stats>().using(team)
            }
            .then(Mono.just(pair.second))
    }

    override fun prepareInitialMessages(): Flux<StatsMessage> = (2015..2021).toFlux()
        .map { season ->
            StatsMessage(page = 1, seasons = listOf(season))
        }
}
