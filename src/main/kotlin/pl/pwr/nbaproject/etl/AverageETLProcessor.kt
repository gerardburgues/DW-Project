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
import pl.pwr.nbaproject.api.AveragesClient
import pl.pwr.nbaproject.model.Queue.AVERAGES
import pl.pwr.nbaproject.model.amqp.SeasonAverageMessage
import pl.pwr.nbaproject.model.api.AveragesWrapper
import pl.pwr.nbaproject.model.db.AVERAGES_TABLE
import pl.pwr.nbaproject.model.db.Average
import reactor.rabbitmq.Receiver
import reactor.rabbitmq.Sender
import kotlin.reflect.KClass

@Service
class AverageETLProcessor(
    rabbitReceiver: Receiver,
    rabbitSender: Sender,
    objectMapper: ObjectMapper,
    r2dbcEntityTemplate: R2dbcEntityTemplate,
    private val averagesClient: AveragesClient,
) : AbstractETLProcessor<SeasonAverageMessage, AveragesWrapper, Average>(
    rabbitReceiver,
    rabbitSender,
    objectMapper,
    r2dbcEntityTemplate,
) {

    override val queue = AVERAGES

    override val tableName: String = AVERAGES_TABLE

    override val messageClass: KClass<SeasonAverageMessage> = SeasonAverageMessage::class

    override suspend fun extract(message: SeasonAverageMessage): AveragesWrapper = with(message) {
        averagesClient.getAverages(playerIds, season)
    }

    override suspend fun transform(data: AveragesWrapper): Pair<List<Average>, Boolean> = data.data.map { average ->
        with(average) {
            Average(
                playerId = playerId,
                season = season,
                gamesPlayed = gamesPlayed,
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
                freeThrowPercentage = freeThrowPercentage
            )
        }
    } to false

    override suspend fun load(data: Pair<List<Average>, Boolean>): Boolean {
        data.first
            .filterNot { average ->
                r2dbcEntityTemplate.select<Average>()
                    .matching(
                        query(
                            where("player_id").isEqual(average.playerId)
                                .and(where("season").isEqual(average.season))
                        )
                    )
                    .exists()
                    .awaitSingle()
            }.map { average ->
                r2dbcEntityTemplate.insert<Average>().usingAndAwait(average)
            }

        return data.second
    }

    override suspend fun feedQueue() {
        //TODO
    }

}
