package pl.pwr.nbaproject.model.db

data class PlayerEntity(
    val gameId: Int,
    val PlayerId: Int,
    val min: String?,
    val teamId: Int,
    val firstname: String?,
    val lastname: String?,
    val pos: String?,
    val points: Int?,
    val totReb: Int?,
    val assist: Int?,
    val stl: Int?,
    val blocks: Int?,
    val turnovers: Int?,

    /**Win 0 Lose 1**/
    val housescore: Int?,
    val visitscore: Int?
)