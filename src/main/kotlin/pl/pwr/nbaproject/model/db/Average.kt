package pl.pwr.nbaproject.model.db

import org.springframework.data.annotation.Id
import org.springframework.data.relational.core.mapping.Table

@Table("averages")
data class Average(
    @Id var playerId: Long,
    var season: Int,
    var gamesPlayed: Int,
    var minutes: String,
    var points: Double,
    var assists: Double,
    var rebounds: Double,
    var defensiveRebounds: Double,
    var offensiveRebounds: Double,
    var blocks: Double,
    var steals: Double,
    var turnovers: Double,
    var personalFouls: Double,
    var fieldGoalsAttempted: Double,
    var fieldGoalsMade: Double,
    var fieldGoalPercentage: Double,
    var threePointersAttempted: Double,
    var threePointersMade: Double,
    var threePointerPercentage: Double,
    var freeThrowsAttempted: Double,
    var freeThrowsMade: Double,
    var freeThrowPercentage: Double,
)
