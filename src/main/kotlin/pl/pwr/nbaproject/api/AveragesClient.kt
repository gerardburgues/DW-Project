package pl.pwr.nbaproject.api

import org.springframework.stereotype.Service
import org.springframework.web.reactive.function.client.WebClient
import org.springframework.web.reactive.function.client.bodyToMono
import pl.pwr.nbaproject.model.api.AveragesWrapper
import reactor.core.publisher.Mono

@Service
class AveragesClient(
    private val ballDontLieWebClient: WebClient,
) {

    fun getAverages(playerIds: List<Long>, season: Int? = null): Mono<AveragesWrapper> = ballDontLieWebClient.get()
        .uri { uriBuilder ->
            uriBuilder.path("/season_averages")

            playerIds.forEach { playerId ->
                uriBuilder.queryParam("player_ids[]", playerId)
            }

            if (season != null) {
                uriBuilder.queryParam("season", season)
            }

            uriBuilder.build()
        }
        .retrieve()
        .bodyToMono()

}
