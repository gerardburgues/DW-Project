package pl.pwr.nbaproject.model.db

import org.springframework.data.relational.core.mapping.Table

@Table("teams")
data class Team(
    var id: Long,
    var abbreviation: String,
    var city: String,
    var conference: Conference,
    var division: Division,
    var fullName: String,
    var name: String,
)
