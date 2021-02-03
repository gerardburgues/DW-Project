package pl.pwr.nbaproject.api

import org.springframework.stereotype.Service
import org.springframework.web.reactive.function.client.WebClient
import org.springframework.web.reactive.function.client.bodyToMono
import pl.pwr.nbaproject.model.api.StatsWrapper
import reactor.core.publisher.Mono

@Service
class StatsClient(
    private val ballDontLieWebClient: WebClient,
) {

    fun getStats(
        seasons: List<Int> = emptyList(),
        teamIds: List<Int> = emptyList(),
        gameIds: List<Int> = emptyList(),
        postSeason: Boolean? = null,
        page: Int = 0,
        perPage: Int = 100
    ): Mono<StatsWrapper> = ballDontLieWebClient.get()
        .uri { uriBuilder ->
            uriBuilder.path("/stats")

            seasons.forEach { season ->
                uriBuilder.queryParam("seasons[]", season)
            }

            teamIds.forEach { teamId ->
                uriBuilder.queryParam("team_ids[]", teamId)
            }

            gameIds.forEach { gameId ->
                uriBuilder.queryParam("game_ids[]", gameId)
            }

            if (postSeason != null) {
                uriBuilder.queryParam("postseason", postSeason)
            }

            uriBuilder.queryParam("page", page)
                .queryParam("per_page", perPage)
                .build()
        }
        .retrieve()
        .bodyToMono()

}
