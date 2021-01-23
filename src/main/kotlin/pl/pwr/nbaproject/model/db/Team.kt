package pl.pwr.nbaproject.model.db

import org.springframework.data.annotation.Id
import org.springframework.data.relational.core.mapping.Table

@Table("teams")
data class Team(
    @Id var id: Long,
    var abbreviation: String,
    var city: String,
    var conference: Conference,
    var division: Division,
    var fullName: String,
    var name: String,
)
