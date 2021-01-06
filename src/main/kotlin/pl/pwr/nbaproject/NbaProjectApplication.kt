package pl.pwr.nbaproject

import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication

@SpringBootApplication
class NbaProjectApplication

fun main(args: Array<String>) {
    runApplication<NbaProjectApplication>(*args)
}
