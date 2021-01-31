package pl.pwr.nbaproject.etl

import org.springframework.beans.factory.InitializingBean
import org.springframework.beans.factory.getBeansOfType
import org.springframework.context.ApplicationContext
import org.springframework.stereotype.Service
import reactor.core.publisher.Flux
import kotlin.time.ExperimentalTime
import kotlin.time.seconds
import kotlin.time.toJavaDuration

@Service
class ETLProcessorRunner(
    private val applicationContext: ApplicationContext
) : InitializingBean {

    @ExperimentalTime
    override fun afterPropertiesSet() {
        val etlProcessors: Map<String, AbstractETLProcessor<*, *, *>> = applicationContext.getBeansOfType()
        val processingStream: Flux<Unit> = Flux.merge(etlProcessors.values.map { it.process() })

        processingStream
            .zipWith(Flux.interval(1.seconds.toJavaDuration()))
            .subscribe()
    }

}
