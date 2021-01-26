package pl.pwr.nbaproject.model.db.functions

import java.math.BigInteger

data class SpecificPlayer(

    var gameId: BigInteger?,
    var points: Int?,
    var assists: Int?,
    var season: Int?,
    var homeTeamScore: Int?,
    var visitorTeamScore: Int?,
    var visitorTeamName: String?,
    var homeTeamName: String?,
    var winnerTeamId: BigInteger?,
    var homeTeamId: BigInteger?,
    var visitorTeamId: BigInteger?

)