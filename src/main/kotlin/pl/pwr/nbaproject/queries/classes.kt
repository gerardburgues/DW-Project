package pl.pwr.nbaproject.queries

data class BPlayer(
    val playerId: Long,
    val firstName: String,
    val lastName: String,
    val position: String,
    val points: Double)

data class Cities(
    val teamId: Long,
    val city: String,
    val points: Double)

data class Height(
    val playerId: Long,
    val heightInches: Int,
    val threePointersMade: Double)

