package pl.pwr.nbaproject.etl

import com.fasterxml.jackson.core.JsonProcessingException
import com.fasterxml.jackson.databind.ObjectMapper
import kotlinx.coroutines.Dispatchers.IO
import kotlinx.coroutines.FlowPreview
import kotlinx.coroutines.reactor.mono
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withContext
import org.apache.logging.log4j.kotlin.Logging
import org.springframework.beans.factory.InitializingBean
import org.springframework.data.r2dbc.core.*
import org.springframework.data.relational.core.query.Criteria.where
import org.springframework.data.relational.core.query.Query.query
import org.springframework.data.relational.core.query.isEqual
import pl.pwr.nbaproject.model.Queue
import pl.pwr.nbaproject.model.db.ETLStatus
import reactor.kotlin.core.publisher.toMono
import reactor.rabbitmq.*
import kotlin.reflect.KClass
import kotlin.time.ExperimentalTime
import kotlin.time.milliseconds
import kotlin.time.seconds
import kotlin.time.toJavaDuration

abstract class AbstractETLProcessor<T1 : Any, T2 : Any, T3 : Any>(
    private val rabbitReceiver: Receiver,
    private val rabbitSender: Sender,
    private val objectMapper: ObjectMapper,
    protected val r2dbcEntityTemplate: R2dbcEntityTemplate,
) : Logging, InitializingBean {

    /**
     * Calls init() method after creation of the object by Spring Dependency Injection Container
     */
    override fun afterPropertiesSet() {
        runBlocking {
            init()
        }
    }

    /**
     * Base method used for performing ETL process. Should be called by subclasses in bean initialization method.
     * Overloaded version using abstract methods.
     */
    open suspend fun init() {
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
    @OptIn(FlowPreview::class, ExperimentalTime::class)
    private suspend fun init(
        queue: Queue,
        toMessage: suspend (AcknowledgableDelivery) -> T1?,
        extract: suspend (T1) -> T2,
        transform: suspend (T2) -> Pair<List<T3>, Boolean>,
        load: suspend (Pair<List<T3>, Boolean>) -> Boolean,
    ) {
        val etlStatus = getEtlStatus()

        if (etlStatus?.done != true) {
            feedQueue()
        }

        val consumeOptions = ConsumeOptions()
            .qos(60)
            .exceptionHandler(
                ExceptionHandlers.RetryAcknowledgmentExceptionHandler(
                    20.seconds.toJavaDuration(),
                    500.milliseconds.toJavaDuration(),
                    ExceptionHandlers.CONNECTION_RECOVERY_PREDICATE
                )
            )

        rabbitReceiver.consumeManualAck(queue.queueName, consumeOptions)
            .flatMap { delivery -> mono { toMessage(delivery) } }
            .flatMap { message -> mono { extract(message) } }
            .flatMap { data -> mono { transform(data) } }
            .flatMap { data -> mono { load(data) } }
            .flatMap { done ->
                mono {
                    if (done) {
                        saveEtlStatus()
                    }
                }
            }

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

    private suspend fun getEtlStatus(): ETLStatus? {
        return r2dbcEntityTemplate
            .select<ETLStatus>()
            .matching(query(where("table_name").isEqual(tableName)))
            .awaitOneOrNull()
    }

    private suspend fun saveEtlStatus() {
        r2dbcEntityTemplate
            .insert<ETLStatus>()
            .usingAndAwait(ETLStatus(tableName, true))
    }

    protected suspend fun sendMessage(message: T1) {
        val body = withContext(IO) {
            objectMapper.writeValueAsBytes(message)
        }
        rabbitSender.send(OutboundMessage("", queue.queueName, body).toMono()).subscribe()
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
     * **Important** Make sure this method **does not** perform any heavy calculations.
     *
     * @param data[T3] transformed data from the [AbstractETLProcessor.transform] step
     * @return list of SQL queries
     */
    abstract suspend fun load(data: Pair<List<T3>, Boolean>): Boolean

    open suspend fun feedQueue() {
        val message = OutboundMessage("", queue.queueName, "{}".encodeToByteArray())
        rabbitSender.send(message.toMono()).subscribe()
    }

}
