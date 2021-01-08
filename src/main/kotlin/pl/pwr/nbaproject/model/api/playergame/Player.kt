package pl.pwr.nbaproject.model.api.playergame

data class Player(
    val first_name: String?,
    val height_feet: Int?,
    val height_inches: Int?,
    val id: Int,
    val last_name: String?,
    val position: String?,
    val team_id: Int,
    val weight_pounds: Int?
)