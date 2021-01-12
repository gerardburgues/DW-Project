package pl.pwr.nbaproject.model.db

enum class Division(
    val conference: Conference
) {
    PACIFIC(Conference.WEST),
    SOUTHEAST(Conference.EAST),
    SOUTHWEST(Conference.WEST),
    ATLANTIC(Conference.EAST),
    NORTHWEST(Conference.WEST),
    CENTRAL(Conference.EAST);
}
