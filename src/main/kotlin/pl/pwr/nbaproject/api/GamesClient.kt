package pl.pwr.nbaproject.api

import org.springframework.stereotype.Service
import org.springframework.web.reactive.function.client.WebClient
import org.springframework.web.reactive.function.client.awaitBody
import pl.pwr.nbaproject.model.api.GamesWrapper

@Service
class GamesClient(
    private val ballDontLieWebClient: WebClient
) {

    suspend fun getGames(
        seasons: List<Int> = emptyList(),
        teamIds: List<Int> = emptyList(),
        postSeason: Boolean? = null,
        page: Int = 0,
        perPage: Int = 100
    ): GamesWrapper = ballDontLieWebClient.get()
        .uri { uriBuilder ->
            uriBuilder.path("/games")

            seasons.forEach { season ->
                uriBuilder.queryParam("seasons[]", season)
            }

            teamIds.forEach { teamId ->
                uriBuilder.queryParam("team_ids[]", teamId)
            }

            if (postSeason != null) {
                uriBuilder.queryParam("postseason", postSeason)
            }

            uriBuilder.queryParam("page", page)
                .queryParam("per_page", perPage)
                .build()
        }
        .retrieve()
        .awaitBody()

}
