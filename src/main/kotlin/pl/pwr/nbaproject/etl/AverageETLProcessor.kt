package pl.pwr.nbaproject.etl

import com.fasterxml.jackson.databind.ObjectMapper
import org.springframework.r2dbc.core.DatabaseClient
import org.springframework.stereotype.Service
import pl.pwr.nbaproject.api.AveragesClient
import pl.pwr.nbaproject.model.Queue
import pl.pwr.nbaproject.model.amqp.SeasonAverageMessage
import pl.pwr.nbaproject.model.api.AveragesWrapper
import pl.pwr.nbaproject.model.db.Average
import reactor.rabbitmq.Receiver
import kotlin.reflect.KClass

@Service
class AverageETLProcessor(
    rabbitReceiver: Receiver,
    objectMapper: ObjectMapper,
    databaseClient: DatabaseClient,
    private val AveragesClient: AveragesClient,
) : AbstractETLProcessor<SeasonAverageMessage, AveragesWrapper, List<Average>>(
    rabbitReceiver,
    objectMapper,
    databaseClient
) {

    override val queue = Queue.AVERAGES

    override val messageClass: KClass<SeasonAverageMessage> = SeasonAverageMessage::class

    override suspend fun extract(apiParams: SeasonAverageMessage): AveragesWrapper = with(apiParams) {
        AveragesClient.getAverages(playerIds, season)
    }

    override suspend fun transform(data: AveragesWrapper): List<Average> = data.data.map { Average ->
        with(Average) {
            Average(
                playerId, season,
                gamesPlayed, minutes,
                points, assists,
                rebounds, defensiveRebounds, offensiveRebounds, blocks,
                steals, turnovers, personalFouls,
                fieldGoalsAttempted, fieldGoalsMade, fieldGoalPercentage,
                threePointersAttempted, threePointersMade, threePointerPercentage,
                freeThrowsAttempted, freeThrowsMade, freeThrowPercentage
            )
        }
    }

    override suspend fun load(data: List<Average>): List<String> = data.map { Average ->
        with(Average) {
            //language=Greenplum
            """
            |INSERT INTO averages(
            |    player_id,
            |    season,
            |    games_played,
            |    "minutes",
            |    points,
            |    assists,
            |    rebounds,
            |    defensive_rebounds
            |    offensive_rebounds,
            |    blocks,
            |    steals,
            |    turnovers,
            |    personal_fouls,
            |    field_goals_attempted,
            |    field_goals_made,
            |    field_goal_percentage,
            |    three_pointers_attempted,
            |    three_pointers_made,
            |    three_pointer_percentage,
            |    free_throws_attempted,
            |    free_throws_made,
            |    free_throw_percentage
            |) 
            |VALUES 
            |(
            |    $playerId,
            |    $season,
            |    $gamesPlayed,
            |    $minutes,
            |    $points,
            |    $assists,
            |    $rebounds,
            |    $defensiveRebounds,
            |    $offensiveRebounds,
            |    $blocks,
            |    $steals,
            |    $turnovers,
            |    $personalFouls,
            |    $fieldGoalsAttempted,
            |    $fieldGoalsMade,
            |    $fieldGoalPercentage,
            |    $threePointersAttempted,
            |    $threePointersMade,
            |    $threePointerPercentage,
            |    $freeThrowsAttempted,
            |    $freeThrowsAttempted,
            |    $fieldGoalPercentage
            |)""".trimMargin()
        }
    }

}
