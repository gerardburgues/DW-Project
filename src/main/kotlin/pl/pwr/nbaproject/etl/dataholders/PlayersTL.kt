package pl.pwr.nbaproject.etl.dataholders

data class PlayersTL(
    val gameId: Long,
    val PlayerId: Long,
    val min: Long,
    val teamId: String,
    val firstname: String,
    val lastname: String,
    val pos: String,
    val points: Long,
    val totReb: Long,
    val assist: Long,
    val stl: Long,
    val blocks: Long,
    val turnovers: Long,
    /**Win 0 Lose 1**/
    val winner: Boolean,
)
