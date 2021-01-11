package pl.pwr.nbaproject.etl

import com.fasterxml.jackson.databind.ObjectMapper
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.reactive.asFlow
import kotlinx.coroutines.reactive.awaitSingle
import org.springframework.r2dbc.core.DatabaseClient
import org.springframework.stereotype.Service
import pl.pwr.nbaproject.api.AverageClient
import pl.pwr.nbaproject.model.Queue
import pl.pwr.nbaproject.model.amqp.PageMessage
import pl.pwr.nbaproject.model.api.playeravg.AveragePlayer
import pl.pwr.nbaproject.model.db.AverageEnitity
import reactor.rabbitmq.Receiver

@Service
class AverageETLProcessor(
    rabbitReceiver: Receiver,
    objectMapper: ObjectMapper,
    private val averageClient: AverageClient,
    private val databaseClient: DatabaseClient,
) : AbstractETLProcessor<PageMessage, AveragePlayer, List<AverageEnitity>>(rabbitReceiver, objectMapper) {

    override val queue: Queue = Queue.PLAYERS
    override val messageClass: Class<PageMessage> = PageMessage::class.java

    override suspend fun extract(apiParams: PageMessage): AveragePlayer {
        return averageClient.getAverage(apiParams.page)
    }

    override suspend fun transform(data: AveragePlayer): List<AverageEnitity> {
        val avg = data.data

        val AverageEntities = mutableListOf<AverageEnitity>()

        for (av in avg) {
            AverageEntities += AverageEnitity(
                Player_Id = av.player_id,
                Season = av.season,
                total_games = av.games_played,
                minpg = av.min,
                ppg = av.pts,
                rebpg = av.reb,
                astpg = av.ast,
                stlpg = av.stl,
                blkpg = av.blk,

                )

        }
        return AverageEntities
    }

    override suspend fun load(data: List<AverageEnitity>) {
        val connection = databaseClient.connectionFactory.create().awaitSingle()
        val batch = connection.createBatch()
        data.forEach {
            //language=Greenplum
            batch.add(
                """INSERT INTO player_avg(player_id, season, total_games, minpg, ppg, rebpg, astpg, stlpg, blkpg
                    |)
                    |VALUES(
                    ${it.Player_Id},${it.Season},${it.total_games},${it.minpg},${it.ppg},${it.rebpg},${it.astpg},${it.stlpg},${it.blkpg}
                    |)""".trimMargin()
            )

        }
        batch.execute().asFlow().collect()
    }

}
