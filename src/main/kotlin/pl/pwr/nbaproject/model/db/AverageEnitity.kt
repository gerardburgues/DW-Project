package pl.pwr.nbaproject.model.db

data class AverageEnitity(


    val Player_Id: Int,
    //val First_Name: String,
    //val Last_Name: String,
    val Season: Int,
    val total_games: Int?,
    val minpg: String?,
    val ppg: Double?,
    /***pg means per game */
    val rebpg: Double?,
    val astpg: Double?,
    val stlpg: Double?,
    val blkpg: Double?,


    )
