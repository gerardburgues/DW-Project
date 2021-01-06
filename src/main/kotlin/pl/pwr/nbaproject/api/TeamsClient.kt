package pl.pwr.nbaproject.api

import org.apache.logging.log4j.kotlin.Logging
import org.springframework.stereotype.Service
import org.springframework.web.reactive.function.client.WebClient
import org.springframework.web.reactive.function.client.awaitBody
import org.springframework.web.reactive.function.client.awaitExchange
import pl.pwr.nbaproject.model.api.Teams

@Service
class TeamsClient(
    private val dataWebClient: WebClient
) : Logging {

    suspend fun getTeams(year: Long): Teams = dataWebClient.get()
        .uri("/prod/v1/${year}/teams.json")
        .awaitExchange {
            logger.debug {
                it.headers().asHttpHeaders()
            }

            it.awaitBody()
        }

}
