package pl.pwr.nbaproject.etl

import com.fasterxml.jackson.databind.ObjectMapper
import org.reactivestreams.Publisher
import org.springframework.data.r2dbc.core.R2dbcEntityTemplate
import org.springframework.data.r2dbc.core.insert
import org.springframework.data.r2dbc.core.select
import org.springframework.data.relational.core.query.Criteria.where
import org.springframework.data.relational.core.query.Query.query
import org.springframework.data.relational.core.query.isEqual
import org.springframework.stereotype.Service
import pl.pwr.nbaproject.api.AveragesClient
import pl.pwr.nbaproject.model.Queue.AVERAGES
import pl.pwr.nbaproject.model.amqp.SeasonAverageMessage
import pl.pwr.nbaproject.model.api.AveragesWrapper
import pl.pwr.nbaproject.model.db.AVERAGES_TABLE
import pl.pwr.nbaproject.model.db.Average
import pl.pwr.nbaproject.model.db.Player
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import reactor.kotlin.core.publisher.toFlux
import reactor.kotlin.core.publisher.toMono
import reactor.kotlin.extra.bool.not
import reactor.rabbitmq.Receiver
import reactor.rabbitmq.Sender
import kotlin.reflect.KClass

@Service
class AverageETLProcessor(
    rabbitReceiver: Receiver,
    rabbitSender: Sender,
    objectMapper: ObjectMapper,
    r2dbcEntityTemplate: R2dbcEntityTemplate,
    private val averagesClient: AveragesClient,
) : AbstractETLProcessor<SeasonAverageMessage, AveragesWrapper, Average>(
    rabbitReceiver,
    rabbitSender,
    objectMapper,
    r2dbcEntityTemplate,
) {

    override val queue = AVERAGES

    override val tableName: String = AVERAGES_TABLE

    override val messageClass: KClass<SeasonAverageMessage> = SeasonAverageMessage::class

    override fun extract(message: SeasonAverageMessage): Mono<AveragesWrapper> = with(message) {
        averagesClient.getAverages(playerIds, season)
    }

    override fun transform(data: AveragesWrapper): Mono<Pair<List<Average>, Boolean>> {
        val pair = data.data.map { average ->
            with(average) {
                Average(
                    playerId = playerId,
                    season = season,
                    gamesPlayed = gamesPlayed,
                    minutes = minutes,
                    points = points,
                    assists = assists,
                    rebounds = rebounds,
                    defensiveRebounds = defensiveRebounds,
                    offensiveRebounds = offensiveRebounds,
                    blocks = blocks,
                    steals = steals,
                    turnovers = turnovers,
                    personalFouls = personalFouls,
                    fieldGoalsAttempted = fieldGoalsAttempted,
                    fieldGoalsMade = fieldGoalsMade,
                    fieldGoalPercentage = fieldGoalPercentage,
                    threePointersAttempted = threePointersAttempted,
                    threePointersMade = threePointersMade,
                    threePointerPercentage = threePointerPercentage,
                    freeThrowsAttempted = freeThrowsAttempted,
                    freeThrowsMade = freeThrowsMade,
                    freeThrowPercentage = freeThrowPercentage
                )
            }
        } to false

        return pair.toMono()
    }

    override fun load(data: Pair<List<Average>, Boolean>): Mono<Boolean> = Flux.fromIterable(data.first)
        .filterWhen { average ->
            r2dbcEntityTemplate.select<Average>()
                .matching(
                    query(
                        where("player_id").isEqual(average.playerId)
                            .and(where("season").isEqual(average.season))
                    )
                )
                .exists()
                .not()
        }
        .flatMap { average ->
            r2dbcEntityTemplate.insert<Average>().using(average).onErrorContinue { e, _ -> }
        }
        .then(data.second.toMono())


    override fun prepareInitialMessages(): Publisher<SeasonAverageMessage> {
        return r2dbcEntityTemplate.select<Player>()
            .all()
            .map { it.id }
            .buffer(100)
            .flatMap { players -> (2015..2021).toFlux().map { season -> SeasonAverageMessage(players, season) } }
    }

}
