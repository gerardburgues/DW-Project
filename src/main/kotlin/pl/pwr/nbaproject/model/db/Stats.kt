package pl.pwr.nbaproject.model.db

import org.springframework.data.relational.core.mapping.Table
import java.time.LocalDate

@Table("stats")
data class Stats(
    var id: Long,
    var playerId: Long,
    var teamId: Long,
    var gameId: Long,
    var homeTeamScore: Int,
    var visitorTeamScore: Int,
    var season: Int,
    var date: LocalDate,
    var firstName: String,
    var lastName: String,
    var minutes: String? = null,
    var points: Int? = null,
    var assists: Int? = null,
    var rebounds: Int? = null,
    var defensiveRebounds: Int? = null,
    var offensiveRebounds: Int? = null,
    var blocks: Int? = null,
    var steals: Int? = null,
    var turnovers: Int? = null,
    var personalFouls: Int? = null,
    var fieldGoalsAttempted: Int? = null,
    var fieldGoalsMade: Int? = null,
    var fieldGoalPercentage: Double? = null,
    var threePointersAttempted: Int? = null,
    var threePointersMade: Int? = null,
    var threePointerPercentage: Double? = null,
    var freeThrowsAttempted: Int? = null,
    var freeThrowsMade: Int? = null,
    var freeThrowPercentage: Double? = null,

    var player: Player? = null,
    var team: Team? = null,
    var game: Game? = null,
)
