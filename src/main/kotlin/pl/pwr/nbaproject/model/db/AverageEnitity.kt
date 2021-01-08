package pl.pwr.nbaproject.model.db

data class AverageEnitity(


    val Player_Id: Int,
    val First_Name: String,
    val Last_Name: String,
    val Season: Int,
    val total_games: Int,
    val minpg: Int,
    val ppg: Int,
    /***pg means per game */
    val rebpg: Int,
    val astpg: Int,
    val stlpg: Int,
    val blkpg: Int,
    val asspg: Int

)
