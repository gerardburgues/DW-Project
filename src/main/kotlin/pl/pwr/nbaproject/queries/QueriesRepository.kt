package pl.pwr.nbaproject.queries

import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.map
import org.springframework.r2dbc.core.DatabaseClient
import org.springframework.r2dbc.core.awaitOne
import org.springframework.r2dbc.core.flow
import org.springframework.stereotype.Component
import pl.pwr.nbaproject.model.db.Player

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


    fun findAll(firstName: String, weightPounds: Int): Flow<Player> {
        return databaseClient.sql("SELECT * FROM players  WHERE first_name = :firstName and weight_pounds >= :weightPounds")
            .bind("firstName", firstName)
            .bind("weightPounds", weightPounds)
            .fetch()
            .flow()
            .map { row: Map<String, Any?> ->
                Player(
                    row["id"] as Long,
                    row["first_name"] as String,
                    row["last_position"] as String,
                    row["position"] as String,
                    row["height_feet"] as Int,
                    row["height_inches"] as Int,
                    row["weight_pounds"] as Int,
                    row["team_id"] as Long
                )
            }
    }

    fun bestPlayer(playerId: Long, firstName: String, lastName: String, position: String, points: Double): Flow<BPlayer> {
        return databaseClient.sql("SELECT best_player()")
            .bind("playerId", playerId)
            .bind("firstName", firstName)
            .bind("lastName", lastName)
            .bind("position", position)
            .bind("points", points)
            .fetch()
            .flow()
            .map { row: Map<String, Any?> ->
                BPlayer(
                    row["player_id"] as Long,
                    row["first_name"] as String,
                    row["last_name"] as String,
                    row["position"] as String,
                    row["points"] as Double,
                 )
            }
}

    fun topCities(teamId: Long, city: String, points: Double): Flow<Cities> {
        return databaseClient.sql("SELECT top_cities()")
            .bind("teamId", teamId)
            .bind("City", city)
            .bind("points", points)
            .fetch()
            .flow()
            .map { row: Map<String, Any?> ->
                Cities(
                    row["team_id"] as Long,
                    row["city"] as String,
                    row["points"] as Double,
                )
            }
    }

    fun corrHeight(firstName: String, lastName: String, heightInches: Int, threePointersMade: Double): Flow<Height> {
        return databaseClient.sql("SELECT corr_height_player()")
            .bind("firstName", firstName)
            .bind("lastName", lastName)
            .bind("heightInches", heightInches)
            .bind("threePointersMade", threePointersMade)
            .fetch()
            .flow()
            .map { row: Map<String, Any?> ->
                Height(
                    row["first_name"] as String
                    row["last_name"] as String,
                    row["height_inches"] as Int,
                    row["three_pointers_made"] as Double,
                )
            }
    }