package pl.pwr.nbaproject.model.db

import org.springframework.data.annotation.Id
import org.springframework.data.relational.core.mapping.Table

@Table("players")
data class Player(
    @Id var id: Long,
    var firstName: String,
    var lastName: String,
    var position: String,
    var heightFeet: Int?,
    var heightInches: Int?,
    var weightPounds: Int?,
    var teamId: Long,

    var team: Team? = null,
)
