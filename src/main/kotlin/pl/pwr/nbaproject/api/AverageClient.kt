package pl.pwr.nbaproject.api

import org.springframework.stereotype.Service
import org.springframework.web.reactive.function.client.WebClient
import org.springframework.web.reactive.function.client.awaitBody
import pl.pwr.nbaproject.model.api.playeravg.AveragePlayer

@Service
class AverageClient(
    private val ballDontLieWebClient: WebClient
) {

    suspend fun getAverage(page: Long, perPage: Long = 100): AveragePlayer = ballDontLieWebClient.get()
        .uri { uriBuilder ->
            uriBuilder.path("/average")
                .queryParam("page", page)
                .queryParam("per_page", perPage)
                .build()
        }
        .retrieve()
        .awaitBody()

}

