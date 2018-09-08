package ir.rkr.darkoob.rest

import com.google.gson.GsonBuilder
import com.typesafe.config.Config
import ir.rkr.darkoob.kafka.KafkaConnector
import ir.rkr.darkoob.util.LayeMetrics
import ir.rkr.darkoob.util.fromJson
import ir.rkr.darkoob.version
import mu.KotlinLogging
import org.eclipse.jetty.http.HttpStatus
import org.eclipse.jetty.server.Server
import org.eclipse.jetty.server.ServerConnector
import org.eclipse.jetty.servlet.ServletContextHandler
import org.eclipse.jetty.servlet.ServletHolder
import org.eclipse.jetty.util.thread.QueuedThreadPool
import javax.servlet.http.HttpServlet
import javax.servlet.http.HttpServletRequest
import javax.servlet.http.HttpServletResponse


/**
 * [Results] is a data model for responses.
 */
data class Results(var results: HashMap<String, String> = HashMap<String, String>())

/**
 * [JettyRestServer] is a rest-based service to handle requests of redis cluster with an additional
 * in-memory cache layer based on ignite to increase performance and decrease number of requests of
 * redis cluster.
 */
class JettyRestServer(val kafka: KafkaConnector, val config: Config, val layemetrics: LayeMetrics) : HttpServlet() {

    private val gson = GsonBuilder().disableHtmlEscaping().create()
    private val logger = KotlinLogging.logger {}

    /**
     * Start a jetty server.
     */
    init {
        val threadPool = QueuedThreadPool(400, 20)
        val server = Server(threadPool)
        val http = ServerConnector(server).apply { port = config.getInt("rest.port") }
        server.addConnector(http)
        val handler = ServletContextHandler(server, "/")

        /**
         * It can handle multi-get requests for Urls in json format.
         */
        handler.addServlet(ServletHolder(object : HttpServlet() {
            override fun doPost(req: HttpServletRequest, resp: HttpServletResponse) {

                val msg = Results()
                val parsedJson = gson.fromJson<Map<String, String>>(req.reader.readText())

                layemetrics.MarkKafkaTotal(1)

                if (parsedJson ==null) {
                    layemetrics.MarkKafkaErrInsert(1)
                    logger.trace { "Null data received." }
                }

                for ((key, value) in parsedJson) {
                    if (!key.isEmpty() and !value.isEmpty()) {
                        kafka.put(key, value)
                        layemetrics.MarkKafkaInsert(1)
                        logger.trace { "A message inserted to kafka key=$key and value=$value" }
                    } else {
                        layemetrics.MarkKafkaErrInsert(1)
                        logger.trace { "key=$key or value=$value is not valid." }
                    }
                }

                resp.apply {
                    status = HttpStatus.OK_200
                    addHeader("Content-Type", "application/json; charset=utf-8")
                    //addHeader("Connection", "close")
                    writer.write(gson.toJson(msg.results))
                }
            }
        }), "/profile")


        handler.addServlet(ServletHolder(object : HttpServlet() {
            override fun doGet(req: HttpServletRequest, resp: HttpServletResponse) {

                resp.apply {
                    status = HttpStatus.OK_200
                    addHeader("Content-Type", "application/json; charset=utf-8")
                    //addHeader("Connection", "close")
                    writer.write(gson.toJson(layemetrics.getInfo()))
                }
            }
        }), "/metrics")


        handler.addServlet(ServletHolder(object : HttpServlet() {
            override fun doGet(req: HttpServletRequest, resp: HttpServletResponse) {
                resp.apply {
                    status = HttpStatus.OK_200
                    addHeader("Content-Type", "text/plain; charset=utf-8")
                    addHeader("Connection", "close")
                    writer.write("Laye V$version is running :D")
                }
            }
        }), "/health")

        server.start()

    }
}