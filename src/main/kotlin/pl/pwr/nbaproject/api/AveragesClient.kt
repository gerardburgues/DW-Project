package pl.pwr.nbaproject.api

import org.springframework.stereotype.Service
import org.springframework.web.reactive.function.client.WebClient
import org.springframework.web.reactive.function.client.awaitBody
import pl.pwr.nbaproject.model.api.AveragesWrapper

@Service
class AveragesClient(
    private val ballDontLieWebClient: WebClient
) {

    suspend fun getAverages(playerIds: List<Long>, season: Int? = null): AveragesWrapper = ballDontLieWebClient.get()
        .uri { uriBuilder ->
            uriBuilder.path("/season_averages")
                .queryParam("season", season)

            playerIds.forEach { playerId ->
                uriBuilder.queryParam("player_ids[]", playerId)
            }

            uriBuilder.build()
        }
        .retrieve()
        .awaitBody()

}
