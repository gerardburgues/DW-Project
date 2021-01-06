package pl.pwr.nbaproject.api

import org.springframework.stereotype.Service
import org.springframework.web.reactive.function.client.WebClient
import org.springframework.web.reactive.function.client.awaitBody

@Service
class PlayersClient(
    private val dataWebClient: WebClient
) {

    suspend fun getPlayers(year: Long): Map<String, Any?> = dataWebClient.get()
        .uri("/prod/v1/$year/players.json")
        .retrieve()
        .awaitBody()

}
