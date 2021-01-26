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
import pl.pwr.nbaproject.api.StatsClient
import pl.pwr.nbaproject.model.Queue
import pl.pwr.nbaproject.model.Queue.STATS
import pl.pwr.nbaproject.model.amqp.StatsMessage
import pl.pwr.nbaproject.model.api.StatsWrapper
import pl.pwr.nbaproject.model.db.STATS_TABLE
import pl.pwr.nbaproject.model.db.Stats
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

    override suspend fun extract(message: StatsMessage): StatsWrapper = with(message) {
        statsClient.getStats(
            seasons,
            teamIds,
            gameIds,
            postSeason,
            page,
            page
        )
    }

    override suspend fun transform(data: StatsWrapper): Pair<List<Stats>, Boolean> {
        if (data.meta.currentPage == 1) {
            for (i in 1 until data.meta.totalPages) {
                sendMessage(StatsMessage(page = i + 1))
            }
        }

        return data.data.map { stats ->
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
        } to (data.meta.currentPage == data.meta.totalPages)
    }

    override suspend fun load(data: Pair<List<Stats>, Boolean>): Boolean {
        data.first
            .filterNot { stats ->
                r2dbcEntityTemplate.select<Stats>()
                    .matching(query(where("id").isEqual(stats.id)))
                    .exists()
                    .awaitSingle()
            }
            .map { stats ->
                r2dbcEntityTemplate.insert<Stats>().usingAndAwait(stats)
            }

        return data.second
    }

}
