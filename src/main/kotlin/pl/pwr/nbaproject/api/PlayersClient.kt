package pl.pwr.nbaproject.api

import org.springframework.stereotype.Service
import org.springframework.web.reactive.function.client.WebClient
import org.springframework.web.reactive.function.client.awaitBody
import pl.pwr.nbaproject.model.api.PlayersWrapper

@Service
class PlayersClient(
    private val ballDontLieWebClient: WebClient
) {

    suspend fun getPlayers(page: Long, perPage: Long = 100): PlayersWrapper = ballDontLieWebClient.get()
        .uri { uriBuilder ->
            uriBuilder.path("/players")
                .queryParam("page", page)
                .queryParam("per_page", perPage)
                .build()
        }
        .retrieve()
        .awaitBody()

}
