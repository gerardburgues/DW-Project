package pl.pwr.nbaproject.etl

import com.fasterxml.jackson.databind.ObjectMapper
import kotlinx.coroutines.Dispatchers.IO
import kotlinx.coroutines.FlowPreview
import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flowOn
import kotlinx.coroutines.flow.launchIn
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.reactive.awaitSingleOrNull
import kotlinx.coroutines.reactor.mono
import kotlinx.coroutines.withContext
import org.apache.logging.log4j.kotlin.Logging
import org.springframework.data.r2dbc.core.*
import org.springframework.data.relational.core.query.Criteria.where
import org.springframework.data.relational.core.query.Query.query
import org.springframework.data.relational.core.query.isEqual
import pl.pwr.nbaproject.model.Queue
import pl.pwr.nbaproject.model.db.ETLStatus
import reactor.core.publisher.Flux
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
    open fun process(): Flux<Unit> {
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
    @OptIn(FlowPreview::class, ExperimentalTime::class)
    private fun process(
        queue: Queue,
        toMessage: suspend (AcknowledgableDelivery) -> T1?,
        extract: suspend (T1) -> T2,
        transform: suspend (T2) -> Pair<List<T3>, Boolean>,
        load: suspend (Pair<List<T3>, Boolean>) -> Boolean,
    ): Flux<Unit> = mono { getEtlStatus() }
        .filter { !it.done }
        .flatMap { mono { sendMessages(prepareInitialMessages()) } }
        .flatMapMany {
            rabbitReceiver.consumeManualAck(
                queue.queueName, ConsumeOptions()
                    .qos(10)
                    .exceptionHandler(
                        ExceptionHandlers.RetryAcknowledgmentExceptionHandler(
                            20.seconds.toJavaDuration(),
                            500.milliseconds.toJavaDuration(),
                            ExceptionHandlers.CONNECTION_RECOVERY_PREDICATE
                        )
                    )
            )
                .flatMap { delivery ->
                    mono { toMessage(delivery) }
                        .flatMap { message ->
                            mono { extract(message) }
                                .retryExponentialBackoff(
                                    times = 3,
                                    first = 10.seconds.toJavaDuration(),
                                    max = 1.minutes.toJavaDuration(),
                                    jitter = true
                                )
                        }
                        .flatMap { data -> mono { transform(data) } }
                        .flatMap { data -> mono { load(data) } }
                        .flatMap { done ->
                            mono {
                                if (done) {
                                    saveEtlStatus()
                                }
                            }
                        }
                        .doOnSuccess {
                            delivery.ack()
                        }.doOnError {
                            delivery.nack(true)
                        }
                }
        }

    /**
     * Jackson ObjectMapper.readValue is a blocking API, I think that this should work fine...
     */
    private suspend fun toMessage(delivery: AcknowledgableDelivery): T1? {
        return try {
            logger.debug {
                "Message: ${delivery.body.decodeToString()}"
            }

        return withContext(IO) {
            objectMapper.readValue(delivery.body, messageClass.java)
        }
    }

    private suspend fun getEtlStatus(): ETLStatus {
        return r2dbcEntityTemplate
            .select<ETLStatus>()
            .matching(query(where("table_name").isEqual(tableName)))
            .awaitOneOrNull() ?: ETLStatus(tableName, false)
    }

    private suspend fun saveEtlStatus() {
        r2dbcEntityTemplate
            .insert<ETLStatus>()
            .usingAndAwait(ETLStatus(tableName, true))
    }

    protected fun sendMessages(messages: Flow<T1>) {
        messages
            .map { objectMapper.writeValueAsBytes(it) }
            .flowOn(IO)
            .map {
                rabbitSender.send(OutboundMessage("", queue.queueName, it).toMono()).awaitSingleOrNull()
            }.launchIn(GlobalScope)
    }

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
    abstract suspend fun extract(message: T1): T2

    /**
     * Method for data transformation from the extraction step to the form acceptable for the data warehouse.
     * **Important** Make sure **all** heavy calculations **are performed here**.
     *
     * @param data[T2] data from [AbstractETLProcessor.extract] step
     * @return [T3] transformed data in the acceptable form for [AbstractETLProcessor.load] step
     */
    abstract suspend fun transform(data: T2): Pair<List<T3>, Boolean>

    /**
     * Method for inserting the data into the data warehouse.
     *
     * @param [data] transformed data from the [AbstractETLProcessor.transform] step
     * @return true if last page has been inserted properly, false otherwise
     */
    abstract suspend fun load(data: Pair<List<T3>, Boolean>): Boolean

    abstract suspend fun prepareInitialMessages(): Flow<T1>

}
