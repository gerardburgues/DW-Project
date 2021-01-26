package pl.pwr.nbaproject.queries

import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.map
import org.springframework.r2dbc.core.DatabaseClient
import org.springframework.r2dbc.core.awaitOne
import org.springframework.r2dbc.core.flow
import org.springframework.stereotype.Component
import pl.pwr.nbaproject.model.db.functions.Best3Pt
import pl.pwr.nbaproject.model.db.functions.CenterPlayer
import pl.pwr.nbaproject.model.db.functions.SortDivision
import pl.pwr.nbaproject.model.db.functions.SpecificPlayer
import java.math.BigInteger

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

    fun center_players(s_minutes: String, s_position: String): Flow<CenterPlayer> {
        val result = databaseClient.sql("SELECT center_player(:s_minutes,:s_position)")
            .bind("s_minutes", s_minutes)
            .bind("s_position", s_position)
            .fetch()
            .flow()
            .map { row: Map<String, Any?> ->
                CenterPlayer(
                    row["minutes"] as String,
                    row["first_name"] as String,
                    row["last_name"] as String,
                    row["v_position"] as String,
                    row["free_throw_percentage"] as Double
                )
            }

        return result
    }

    fun show_best_3_pt(a_percentage: Double): Flow<Best3Pt> {
        val result = databaseClient.sql("SELECT show_best_3_pt(:a_percentage)")
            .bind("a_percentage", a_percentage)

            .fetch()
            .flow()
            .map { row: Map<String, Any?> ->
                Best3Pt(
                    row["three_pointer_percentage"] as Double
                )
            }

        return result
    }

    fun show_specific_players(s_points: Int, s_assists: Int): Flow<SpecificPlayer> {
        val result = databaseClient.sql("SELECT show_specific_players(:s_points,:s_assists)")
            .bind("s_points", s_points)
            .bind("s_assits", s_assists)
            .fetch()
            .flow()
            .map { row: Map<String, Any?> ->
                SpecificPlayer(
                    row["game_id"] as BigInteger,
                    row["points"] as Int,
                    row["assists"] as Int,
                    row["season"] as Int,
                    row["home_team_score"] as Int,
                    row["visitor_team_score"] as Int,
                    row["visitor_team_name"] as String,
                    row["home_team_name"] as String,
                    row["winner_team_id"] as BigInteger,
                    row["home_team_id"] as BigInteger,
                    row["visitor_team_id"] as BigInteger
                )
            }

        return result
    }

    fun sort_by_division(s_points: Int, s_assists: Int): Flow<SortDivision> {
        val result = databaseClient.sql("SELECT show_specific_players(:s_points,:s_assists)")
            .bind("s_points", s_points)
            .bind("s_assits", s_assists)
            .fetch()
            .flow()
            .map { row: Map<String, Any?> ->
                SortDivision(
                    row["points"] as Int,
                    row["assists"] as Int,
                    row["first_name"] as String,
                    row["last_name"] as String,
                    row["division"] as String,

                    )
            }

        return result
    }
}
