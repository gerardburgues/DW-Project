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
import pl.pwr.nbaproject.model.Queue
import pl.pwr.nbaproject.model.amqp.SeasonAverageMessage
import pl.pwr.nbaproject.model.api.AveragesWrapper
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

    override val queue = Queue.AVERAGES

    override val messageClass: KClass<SeasonAverageMessage> = SeasonAverageMessage::class

    override suspend fun extract(apiParams: SeasonAverageMessage): AveragesWrapper = with(apiParams) {
        averagesClient.getAverages(playerIds, season)
    }

    override suspend fun transform(data: AveragesWrapper): List<Average> = data.data.map { Average ->
        with(Average) {
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
    }

    override suspend fun load(data: List<Average>) {
        data
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
    }

}
