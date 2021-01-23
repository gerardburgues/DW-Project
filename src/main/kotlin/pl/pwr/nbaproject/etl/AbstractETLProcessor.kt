package pl.pwr.nbaproject.etl

import com.fasterxml.jackson.core.JsonProcessingException
import com.fasterxml.jackson.databind.ObjectMapper
import io.r2dbc.spi.Result
import kotlinx.coroutines.Dispatchers.IO
import kotlinx.coroutines.FlowPreview
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.reactive.asFlow
import kotlinx.coroutines.reactive.awaitSingle
import kotlinx.coroutines.reactor.mono
import kotlinx.coroutines.withContext
import org.apache.logging.log4j.kotlin.Logging
import org.springframework.beans.factory.InitializingBean
import org.springframework.r2dbc.core.DatabaseClient
import pl.pwr.nbaproject.model.Queue
import reactor.rabbitmq.AcknowledgableDelivery
import reactor.rabbitmq.ConsumeOptions
import reactor.rabbitmq.ExceptionHandlers
import reactor.rabbitmq.Receiver
import java.time.Duration
import kotlin.reflect.KClass

abstract class AbstractETLProcessor<T1 : Any, T2, T3>(
    private val rabbitReceiver: Receiver,
    private val objectMapper: ObjectMapper,
    private val databaseClient: DatabaseClient,
) : Logging, InitializingBean {

    /**
     * Calls init() method after creation of the object by Spring Dependency Injection Container
     */
    override fun afterPropertiesSet() {
        init()
    }

    /**
     * Base method used for performing ETL process. Should be called by subclasses in bean initialization method.
     * Overloaded version using abstract methods.
     */
    open fun init() {
        this.init(queue, ::toMessage, ::extract, ::transform, ::load)
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
    @OptIn(FlowPreview::class)
    private fun init(
        queue: Queue,
        toMessage: suspend (AcknowledgableDelivery) -> T1?,
        extract: suspend (T1) -> T2,
        transform: suspend (T2) -> T3,
        load: suspend (T3) -> List<String>,
    ) {
        rabbitReceiver.consumeManualAck(
            queue.queueName,
            ConsumeOptions().qos(60).exceptionHandler { context, exception ->
                ExceptionHandlers.RetryAcknowledgmentExceptionHandler(
                    Duration.ofSeconds(20), Duration.ofMillis(500),
                    ExceptionHandlers.CONNECTION_RECOVERY_PREDICATE
                )
            })
            .flatMap { delivery -> mono { toMessage(delivery) } }
            .flatMap { message -> mono { extract(message) } }
            .flatMap { data -> mono { transform(data) } }
            .flatMap { data -> mono { load(data) } }
            .flatMap { queries -> mono { executeQueries(queries) } }
            .subscribe()
    }

    /**
     * Jackson ObjectMapper.readValue is a blocking API, I think that this should work fine...
     */
    private suspend fun toMessage(delivery: AcknowledgableDelivery): T1? {
        return try {
            logger.debug {
                "Message: ${delivery.body.decodeToString()}"
            }

            val mappedMessage = withContext(IO) {
                objectMapper.readValue(delivery.body, messageClass.java)
            }

            delivery.ack()
            mappedMessage
        } catch (e: JsonProcessingException) {
            logger.error(e)
            delivery.nack(false)
            null
        }
    }

    /**
     * Queries should be properly escaped, but for performance we ignore possible SQL-injection problems
     */
    private suspend fun executeQueries(queries: List<String>): Flow<Result> {
        val batch = databaseClient.connectionFactory.create().awaitSingle().createBatch()
        queries.forEach { query -> batch.add(query) }
        return batch.execute().asFlow()
    }

    /**
     * Determine queue type for polling only proper messages
     */
    abstract val queue: Queue

    /**
     * Workaround for lack of support for dynamical type resolution on JVM
     */
    abstract val messageClass: KClass<T1>

    /**
     * Method for data fetching from API.
     * **Important** Make sure this method **does not** perform any heavy calculations.
     *
     * @param apiParams[T1] message from RabbitMQ as a properly typed object
     * @return [T2] data fetched from NBA API in the acceptable form for [AbstractETLProcessor.transform]
     */
    abstract suspend fun extract(apiParams: T1): T2

    /**
     * Method for data transformation from the extraction step to the form acceptable for the data warehouse.
     * **Important** Make sure **all** heavy calculations **are performed here**.
     *
     * @param data[T2] data from [AbstractETLProcessor.extract] step
     * @return [T3] transformed data in the acceptable form for [AbstractETLProcessor.load] step
     */
    abstract suspend fun transform(data: T2): T3

    /**
     * Method for inserting the data into the data warehouse.
     * **Important** Make sure this method **does not** perform any heavy calculations.
     *
     * @param data[T3] transformed data from the [AbstractETLProcessor.transform] step
     * @return list of SQL queries
     */
    abstract suspend fun load(data: T3): List<String>

}
