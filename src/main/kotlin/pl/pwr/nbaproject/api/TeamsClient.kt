package pl.pwr.nbaproject.api

import org.springframework.stereotype.Service
import org.springframework.web.reactive.function.client.WebClient
import org.springframework.web.reactive.function.client.bodyToMono
import pl.pwr.nbaproject.model.api.TeamsWrapper
import reactor.core.publisher.Mono

@Service
class TeamsClient(
    private val ballDontLieWebClient: WebClient,
) {

    fun getTeams(page: Int = 0, perPage: Int = 30): Mono<TeamsWrapper> = ballDontLieWebClient.get()
        .uri { uriBuilder ->
            uriBuilder.path("/teams")
                .queryParam("page", page)
                .queryParam("per_page", perPage)
                .build()
        }
        .retrieve()
        .bodyToMono()

}
