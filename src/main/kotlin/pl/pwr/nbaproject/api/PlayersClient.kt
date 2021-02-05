package pl.pwr.nbaproject.api

import org.springframework.stereotype.Service
import org.springframework.web.reactive.function.client.WebClient
import org.springframework.web.reactive.function.client.bodyToMono
import pl.pwr.nbaproject.model.api.PlayersWrapper
import reactor.core.publisher.Mono

@Service
class PlayersClient(
    private val ballDontLieWebClient: WebClient,
) {

    fun getPlayers(page: Int = 0, perPage: Int = 100): Mono<PlayersWrapper> = ballDontLieWebClient.get()
        .uri { uriBuilder ->
            uriBuilder.path("/players")
                .queryParam("page", page)
                .queryParam("per_page", perPage)
                .build()
        }
        .retrieve()
        .bodyToMono<PlayersWrapper>()

}
