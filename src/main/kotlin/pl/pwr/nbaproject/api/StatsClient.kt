package pl.pwr.nbaproject.api

import org.springframework.stereotype.Service
import org.springframework.web.reactive.function.client.WebClient
import org.springframework.web.reactive.function.client.awaitBody
import pl.pwr.nbaproject.model.api.StatsWrapper

@Service
class StatsClient(
    private val ballDontLieWebClient: WebClient
) {

    suspend fun getStats(page: Long, perPage: Long = 100): StatsWrapper = ballDontLieWebClient.get()
        .uri { uriBuilder ->
            uriBuilder.path("/stats")
                .queryParam("page", page)
                .queryParam("per_page", perPage)
                .build()
        }
        .retrieve()
        .awaitBody()

}
