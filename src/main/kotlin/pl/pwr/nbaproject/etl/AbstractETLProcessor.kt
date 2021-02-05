package pl.pwr.nbaproject.etl

import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.logging.log4j.kotlin.Logging
import org.reactivestreams.Publisher
import org.springframework.data.r2dbc.core.R2dbcEntityTemplate
import org.springframework.data.r2dbc.core.insert
import org.springframework.data.r2dbc.core.select
import org.springframework.data.relational.core.query.Criteria.where
import org.springframework.data.relational.core.query.Query.query
import org.springframework.data.relational.core.query.isEqual
import pl.pwr.nbaproject.model.Queue
import pl.pwr.nbaproject.model.db.ETLStatus
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import reactor.core.publisher.SignalType
import reactor.kotlin.core.publisher.switchIfEmpty
import reactor.kotlin.core.publisher.toMono
import reactor.kotlin.extra.retry.retryExponentialBackoff
import reactor.rabbitmq.*
import kotlin.reflect.KClass
import kotlin.time.*

abstract class AbstractETLProcessor<T1 : Any, T2 : Any, T3 : Any>(
    private val rabbitReceiver: Receiver,
    private val rabbitSender: Sender,
    private val objectMapper: ObjectMapper,
    protected val r2dbcEntityTemplate: R2dbcEntityTemplate,
) : Logging {

    /**
     * Base method used for performing ETL process. Should be called by subclasses in bean initialization method.
     * Overloaded version using abstract methods.
     */
    open fun process(): Flux<*> {
        return this.process(queue, ::toMessage, ::extract, ::transform, ::load)
    }

    /**
     * Base method used for performing ETL process. Should be called by subclasses in bean initialization method.
     * This method rather should not be used directly.
     *
     * @param queue queue name for message polling
     * @param extract function for data transformation from the extraction step to the form acceptable for the data warehouse
     * @param transform function for data transformation from the extraction step to the form acceptable for the data warehouse
     * @param load function for inserting the data into the data warehouse
     */
    @OptIn(ExperimentalTime::class)
    private fun process(
        queue: Queue,
        toMessage: (AcknowledgableDelivery) -> Mono<T1>,
        extract: (T1) -> Mono<T2>,
        transform: (T2) -> Mono<Pair<List<T3>, Boolean>>,
        load: (Pair<List<T3>, Boolean>) -> Mono<Boolean>,
    ): Flux<*> {
        val flux1 = getEtlStatus()
            .filter { !it.done }
            .flatMap { sendMessages(prepareInitialMessages()) }

        val flux2 = rabbitReceiver.consumeManualAck(
            queue.queueName, ConsumeOptions()
                .qos(1)
                .exceptionHandler(
                    ExceptionHandlers.RetryAcknowledgmentExceptionHandler(
                        20.seconds.toJavaDuration(),
                        500.milliseconds.toJavaDuration(),
                        ExceptionHandlers.CONNECTION_RECOVERY_PREDICATE
                    )
                )
        )
            .delayElements(1.minutes.toJavaDuration())
            .flatMap { delivery ->
                toMessage(delivery)
                    .flatMap { message ->
                        extract(message)
                            .retryExponentialBackoff(
                                times = 3,
                                first = 10.seconds.toJavaDuration(),
                                max = 1.minutes.toJavaDuration(),
                                jitter = true
                            )
                    }
                    .flatMap { data -> transform(data) }
                    .flatMap { data -> load(data) }
                    .flatMap { done ->
                        if (done) {
                            saveEtlStatus()
                        } else {
                            Mono.just(1)
                        }
                    }
                    .doFinally {
                        if (it == SignalType.ON_COMPLETE) {
                            delivery.ack()
                        } else {
                            delivery.nack(true)
                        }
                    }
            }

        return flux1.thenMany(flux2)
    }

    /**
     * Jackson ObjectMapper.readValue is a blocking API, I think that this should work fine...
     */
    private fun toMessage(delivery: AcknowledgableDelivery): Mono<T1> {
        logger.debug("Message: ${delivery.body.decodeToString()}")

        return delivery.toMono().map {
            objectMapper.readValue(it.body, messageClass.java)
        }
    }

    private fun getEtlStatus(): Mono<ETLStatus> {
        return r2dbcEntityTemplate
            .select<ETLStatus>()
            .matching(query(where("table_name").isEqual(tableName)))
            .one()
            .switchIfEmpty { ETLStatus(tableName, false).toMono() }
    }

    private fun saveEtlStatus(): Mono<*> {
        return r2dbcEntityTemplate
            .insert<ETLStatus>()
            .using(ETLStatus(tableName, true))
    }

    protected fun sendMessages(messages: Publisher<T1>): Mono<Void> = Flux.from(messages)
        .map { OutboundMessage("", queue.queueName, objectMapper.writeValueAsBytes(it)) }
        .`as`(rabbitSender::send)

    /**
     * Determine queue type for polling only proper messages
     */
    abstract val queue: Queue

    abstract val tableName: String

    /**
     * Workaround for lack of support for dynamical type resolution on JVM
     */
    abstract val messageClass: KClass<T1>

    /**
     * Method for data fetching from API.
     * **Important** Make sure this method **does not** perform any heavy calculations.
     *
     * @param message[T1] message from RabbitMQ as a properly typed object
     * @return [T2] data fetched from NBA API in the acceptable form for [AbstractETLProcessor.transform]
     */
    abstract fun extract(message: T1): Mono<T2>

    /**
     * Method for data transformation from the extraction step to the form acceptable for the data warehouse.
     * **Important** Make sure **all** heavy calculations **are performed here**.
     *
     * @param data[T2] data from [AbstractETLProcessor.extract] step
     * @return [T3] transformed data in the acceptable form for [AbstractETLProcessor.load] step
     */
    abstract fun transform(data: T2): Mono<Pair<List<T3>, Boolean>>

    /**
     * Method for inserting the data into the data warehouse.
     *
     * @param [data] transformed data from the [AbstractETLProcessor.transform] step
     * @return true if last page has been inserted properly, false otherwise
     */
    abstract fun load(data: Pair<List<T3>, Boolean>): Mono<Boolean>

    abstract fun prepareInitialMessages(): Publisher<T1>

}
