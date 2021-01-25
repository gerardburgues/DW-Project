package pl.pwr.nbaproject.queries

import org.springframework.r2dbc.core.DatabaseClient
import org.springframework.r2dbc.core.awaitOne
import org.springframework.stereotype.Component

@Component
class QueriesRepository(
    val databaseClient: DatabaseClient,
) {

    suspend fun findWinnerId(gameId: Long): Long {
        val result = databaseClient.sql("SELECT find_winner_id(:gameId)")
            .bind("gameId", gameId)
            .fetch()
            .awaitOne()

        return result["find_winner_id"] as Long
    }

    suspend fun center_player(gameId: Long): Long {
        val result = databaseClient.sql("SELECT find_winner_id(:gameId)")
            .bind("gameId", gameId)
            .fetch()
            .awaitOne()

        return result["find_winner_id"] as Long
    }
}
