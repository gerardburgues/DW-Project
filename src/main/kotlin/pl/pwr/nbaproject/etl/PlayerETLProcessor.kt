package pl.pwr.nbaproject.etl

import com.fasterxml.jackson.databind.ObjectMapper
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.reactive.asFlow
import kotlinx.coroutines.reactive.awaitSingle
import org.springframework.r2dbc.core.DatabaseClient
import org.springframework.stereotype.Service
import pl.pwr.nbaproject.api.PlayersClient
import pl.pwr.nbaproject.model.Queue
import pl.pwr.nbaproject.model.amqp.PageMessage
import pl.pwr.nbaproject.model.api.playergame.Players
import pl.pwr.nbaproject.model.db.PlayerEntity
import reactor.rabbitmq.Receiver

@Service
class PlayerETLProcessor(
    rabbitReceiver: Receiver,
    objectMapper: ObjectMapper,
    private val playersClient: PlayersClient,
    private val databaseClient: DatabaseClient,
) : AbstractETLProcessor<PageMessage, Players, List<PlayerEntity>>(rabbitReceiver, objectMapper) {
    override val queue: Queue = Queue.PLAYERS
    override val messageClass: Class<PageMessage> = PageMessage::class.java

    /*    override suspend fun extract(apiParams: YearMessage): Players {
            return playersClient.getPlayers(apiParams.year)
        }*/
    override suspend fun extract(apiParams: PageMessage): Players {
        return playersClient.getPlayers(apiParams.page)
    }

    override suspend fun transform(data: Players): List<PlayerEntity> {

        val playerss = data.data
        val playerEntities = mutableListOf<PlayerEntity>()

        for (player in playerss) {

            playerEntities += PlayerEntity(
                gameId = player.game.id,
                PlayerId = player.player.id,
                min = player.min,
                teamId = player.team.id,
                firstname = player.player.first_name,
                lastname = player.player.last_name,
                pos = player.player.position,
                points = player.pts,
                totReb = player.reb,
                assist = player.ast,
                stl = player.stl,
                blocks = player.blk,
                turnovers = player.turnover,
                housescore = player.game.home_team_score,
                visitscore = player.game.visitor_team_score


            )
        }

        return playerEntities
    }

    override suspend fun load(data: List<PlayerEntity>) {
        val connection = databaseClient.connectionFactory.create().awaitSingle()
        val batch = connection.createBatch()
        data.forEach {
            //language=Greenplum
            batch.add(
                """INSERT INTO players_game (
                    gameid, playerid, min, teamid, firstname, lastname, pos,points, totreb, assist, stl, blocks, turnovers,housescore,visitscore
                    ) 
                    VALUES (
                    ${it.gameId},${it.PlayerId},${it.min},${it.teamId},${it.firstname},${it.lastname},${it.pos},${it.points},${it.totReb},${it.assist},${it.stl},${it.blocks},${it.turnovers},${it.housescore},${it.visitscore}
                    ) 
                    """.trimIndent()
            )

        }
        batch.execute().asFlow().collect()
    }
}
