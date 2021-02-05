package pl.pwr.nbaproject.etl

import org.apache.logging.log4j.kotlin.Logging
import org.springframework.beans.factory.InitializingBean
import org.springframework.beans.factory.getBeansOfType
import org.springframework.context.ApplicationContext
import org.springframework.stereotype.Service
import reactor.core.publisher.Flux
import kotlin.time.ExperimentalTime

@Service
class ETLProcessorRunner(
    private val applicationContext: ApplicationContext
) : InitializingBean, Logging {

    @ExperimentalTime
    override fun afterPropertiesSet() {
        val etlProcessors: Map<String, AbstractETLProcessor<*, *, *>> = applicationContext.getBeansOfType()
        val processingStream: Flux<*> = Flux.merge(etlProcessors.values.map { it.process() })

        processingStream
            .subscribe()
    }

}
