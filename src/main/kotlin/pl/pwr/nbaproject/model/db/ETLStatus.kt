package pl.pwr.nbaproject.model.db

import org.springframework.data.annotation.Id
import org.springframework.data.relational.core.mapping.Table

@Table(ETL_STATUS_TABLE)
data class ETLStatus(
    @Id var tableName: String,
    var done: Boolean,
)
