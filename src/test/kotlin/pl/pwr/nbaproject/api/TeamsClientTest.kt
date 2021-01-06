package pl.pwr.nbaproject.api

import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import pl.pwr.nbaproject.model.amqp.YearMessage

@SpringBootTest
internal class TeamsClientTest {

    @Autowired
    private lateinit var teamsClient: TeamsClient

    @Test
    fun getTeams() {
        runBlocking {
            val teams = teamsClient.getTeams(YearMessage(2019))
            assertEquals(teams.league["standard"]?.size ?: 0, 44)
        }
    }

}
