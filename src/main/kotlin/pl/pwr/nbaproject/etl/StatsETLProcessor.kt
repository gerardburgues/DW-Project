package pl.pwr.nbaproject.etl

import com.fasterxml.jackson.databind.ObjectMapper
import org.springframework.r2dbc.core.DatabaseClient
import org.springframework.stereotype.Service
import pl.pwr.nbaproject.api.StatsClient
import pl.pwr.nbaproject.model.Queue
import pl.pwr.nbaproject.model.amqp.PageMessage
import pl.pwr.nbaproject.model.api.StatsWrapper
import pl.pwr.nbaproject.model.db.Stats
import reactor.rabbitmq.Receiver

@Service
class StatsETLProcessor(
    rabbitReceiver: Receiver,
    objectMapper: ObjectMapper,
    databaseClient: DatabaseClient,
    private val statsClient: StatsClient,
) : AbstractETLProcessor<PageMessage, StatsWrapper, List<Stats>>(rabbitReceiver, objectMapper, databaseClient) {

    override val queue: Queue = Queue.PLAYERS

    override val messageClass: Class<PageMessage> = PageMessage::class.java

    override suspend fun extract(apiParams: PageMessage): StatsWrapper {
        return statsClient.getStats(apiParams.page)
    }

    override suspend fun transform(data: StatsWrapper): List<Stats> = data.data.map { stats ->
        with(stats) {
            Stats(
                id,
                player.id,
                team.id,
                game.id,
                minutes,
                points,
                assists,
                rebounds,
                defensiveRebounds,
                offensiveRebounds,
                blocks,
                steals,
                turnovers,
                personalFouls,
                fieldGoalsAttempted,
                fieldGoalsMade,
                fieldGoalPercentage,
                threePointersAttempted,
                threePointersMade,
                threePointerPercentage,
                freeThrowsAttempted,
                freeThrowsMade,
                freeThrowPercentage
            )
        }
    }

    override suspend fun load(data: List<Stats>): List<String> = data.map { stats ->
        with(stats) {
            //language=Greenplum
            """
            |INSERT INTO stats
            |(
            |    id,
            |    player_id,
            |    team_id,
            |    game_id, 
            |    "minutes",
            |    points,
            |    assists,
            |    rebounds,
            |    defensive_rebounds,
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
            |    $id,
            |    $playerId,
            |    $teamId,
            |    $gameId,
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
            |    $freeThrowsMade,
            |    $freeThrowPercentage
            |)""".trimMargin()
        }
    }

}
