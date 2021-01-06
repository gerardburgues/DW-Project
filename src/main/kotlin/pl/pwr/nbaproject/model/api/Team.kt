package pl.pwr.nbaproject.model.api

data class Team(
    val altCityName: String?,
    val city: String?,
    val confName: String?,
    val divName: String?,
    val fullName: String?,
    val isAllStar: Boolean,
    val isNBAFranchise: Boolean,
    val nickname: String?,
    val teamId: Long,
    val teamShortName: String?,
    val tricode: String?,
    val urlName: String?
)
