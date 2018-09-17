package ir.rkr.darkoob.rest

import com.google.common.hash.Hashing
import com.google.gson.GsonBuilder
import com.typesafe.config.Config
import ir.rkr.darkoob.hbase.HbaseConnector
import ir.rkr.darkoob.kafka.KafkaConnector
import ir.rkr.darkoob.util.DMetric
import ir.rkr.darkoob.version
import mu.KotlinLogging
import org.eclipse.jetty.http.HttpStatus
import org.eclipse.jetty.server.Server
import org.eclipse.jetty.server.ServerConnector
import org.eclipse.jetty.servlet.ServletContextHandler
import org.eclipse.jetty.servlet.ServletHolder
import org.eclipse.jetty.util.thread.QueuedThreadPool
import java.io.ObjectOutputStream
import java.nio.ByteBuffer
import java.util.*
import javax.servlet.http.HttpServlet
import javax.servlet.http.HttpServletRequest
import javax.servlet.http.HttpServletResponse


/**
 * [Results] is a data model for responses.
 */
data class Results(var results: HashMap<ByteArray, ByteArray> = HashMap<ByteArray, ByteArray>())

fun strToLongByte(input: String, ts: Boolean = false): ByteArray {

    if (ts) return ByteBuffer.allocate(java.lang.Long.BYTES).putLong(4102444800000L - input.toLong()).array()
    return ByteBuffer.allocate(java.lang.Long.BYTES).putLong(input.toLong()).array()
}

/**
 * [JettyRestServer] is a rest-based service to handle requests of redis cluster with an additional
 * in-memory cache layer based on ignite to increase performance and decrease number of requests of
 * redis cluster.
 */
class JettyRestServer(val config: Config, val dMetric: DMetric) : HttpServlet() {

    private val gson = GsonBuilder().disableHtmlEscaping().create()
    private val logger = KotlinLogging.logger {}
    private val murmur = Hashing.murmur3_32()


    /**
     * Start a jetty server.
     */
    init {
        val threadPool = QueuedThreadPool(400, 20)
        val server = Server(threadPool)
        val http = ServerConnector(server).apply { port = config.getInt("rest.port") }
        server.addConnector(http)
        val handler = ServletContextHandler(server, "/")

        val pfKafka = KafkaConnector("pf", config)
        val pfTable = HbaseConnector("pf", config, dMetric)

        val tkKafka = KafkaConnector("tk", config)
        val tkTable = HbaseConnector("tk", config, dMetric)

        val chKafka = KafkaConnector("ch", config)
        val chTable = HbaseConnector("ch", config, dMetric)

        val k2kKafka = KafkaConnector("k2k", config)
        val k2kTable = HbaseConnector("k2k", config, dMetric)


        /**PF
         **************************************************************************************************************/
        handler.addServlet(ServletHolder(object : HttpServlet() {

            /**
             * to post binary data to kafka
             */
            override fun doPost(req: HttpServletRequest, resp: HttpServletResponse) {

                val kafkaValue = req.inputStream.readBytes(req.contentLength)
                val key = strToLongByte(req.getParameter("key"))

                val salt = murmur.hashBytes(key).asBytes()
                val kafkaKey = ByteBuffer.allocate(salt.size + key.size).put(salt).put(key).array()

                dMetric.MarkKafkaTotal(1)

                try {
                    pfKafka.put(kafkaKey, kafkaValue)
                    dMetric.MarkKafkaInsert(1)
                } catch (e: Exception) {
                    logger.trace { e }
                    dMetric.MarkKafkaErrInsert(1)
                }

                resp.apply {
                    status = HttpStatus.OK_200
                    addHeader("Content-Type", "application/json; charset=utf-8")
                }
            }

            /**
             * to Retrieve data from Hbase
             */

            override fun doGet(req: HttpServletRequest, resp: HttpServletResponse) {

                val key = strToLongByte(req.getParameter("key"))

//                logger.trace { "key=$key value=$value" }
//                dMetric.MarkKafkaTotal(1)
                val result = Results()

                try {
                    val salt = murmur.hashBytes(key).asBytes()
                    val rowKey = ByteBuffer.allocate(salt.size + key.size).put(salt).put(key).array()

                    result.results[key] = pfTable.get(rowKey, "cf", "q")

//                    dMetric.MarkKafkaInsert(1)
                } catch (e: Exception) {
                    logger.trace { e }
//                    dMetric.MarkKafkaErrInsert(1)
//                    logger.trace { "key=$key or ts=$ts orvalue=$value is not valid." }
                }

                resp.apply {
                    status = HttpStatus.OK_200
                    addHeader("Content-Type", "application/x-binary")
                    //       writer.write(String(pfTable.get(key.toByteArray(),"cf","q")))
                    val os = resp.outputStream
                    val oos = ObjectOutputStream(os)
                    oos.writeObject(result.results)
                }
            }
        }), "/pf")


        /**TK
         **************************************************************************************************************/
        handler.addServlet(ServletHolder(object : HttpServlet() {

            /**
             * to post binary data to kafka
             */
            override fun doPost(req: HttpServletRequest, resp: HttpServletResponse) {

                val kafkaValue = req.inputStream.readBytes(req.contentLength)
                println(kafkaValue.size)
                val key = Base64.getDecoder().decode(req.getParameter("key"))
                val devid = Base64.getDecoder().decode(req.getParameter("devid"))

                val devId = murmur.hashBytes(devid).asBytes()
                val salt = murmur.hashBytes(key).asBytes()
                val kafkaKey = ByteBuffer.allocate(salt.size + key.size + devId.size).put(salt).put(key).put(devId).array()

                dMetric.MarkKafkaTotal(1)

                try {
                    tkKafka.put(kafkaKey, kafkaValue)
                    dMetric.MarkKafkaInsert(1)
                } catch (e: Exception) {
                    logger.trace { e }
                    dMetric.MarkKafkaErrInsert(1)
                }

                resp.apply {
                    status = HttpStatus.OK_200
                    addHeader("Content-Type", "application/json; charset=utf-8")
                }
            }

            /**
             * to Retrieve data from Hbase
             */

            override fun doGet(req: HttpServletRequest, resp: HttpServletResponse) {

                val key = Base64.getDecoder().decode(req.getParameter("key"))

//                logger.trace { "key=$key value=$value" }
//                dMetric.MarkKafkaTotal(1)
                val result = Results()

                try {
                    val salt = murmur.hashBytes(key).asBytes()
                    val rowKey = ByteBuffer.allocate(salt.size + key.size).put(salt).put(key).array()

                    tkTable.scanWithPrefix(rowKey).forEach { it ->
                        result.results[it.key] = it.value;
                        println(it.value.size)
                    }

//                    dMetric.MarkKafkaInsert(1)
                } catch (e: Exception) {
                    logger.trace { e }
//                    dMetric.MarkKafkaErrInsert(1)
//                    logger.trace { "key=$key or ts=$ts orvalue=$value is not valid." }
                }

                resp.apply {
                    status = HttpStatus.OK_200
                    addHeader("Content-Type", "application/x-binary")
//                           writer.write(gson.toJson(result.results).toByteArray(Charsets.UTF_8))
                    val os = resp.outputStream
                    val oos = ObjectOutputStream(os)
                    oos.writeObject(result.results)
                }
            }
        }), "/tk")


        /**CH
         **************************************************************************************************************/
        handler.addServlet(ServletHolder(object : HttpServlet() {

            /**
             * to post binary data to kafka
             */
            override fun doPost(req: HttpServletRequest, resp: HttpServletResponse) {

                val kafkaValue = req.inputStream.readBytes(req.contentLength)
                val key = strToLongByte(req.getParameter("key"))
                var ts = strToLongByte(System.currentTimeMillis().toString(), true)

                if (!req.getParameter("ts").isNullOrBlank())
                    ts = strToLongByte(req.getParameter("ts"), true)

                val salt = murmur.hashBytes(key).asBytes()
                val kafkaKey = ByteBuffer.allocate(salt.size + key.size + ts.size).put(salt).put(key).put(ts).array()

                dMetric.MarkKafkaTotal(1)

                try {
                    chKafka.put(kafkaKey, kafkaValue)
                    dMetric.MarkKafkaInsert(1)
                } catch (e: Exception) {
                    logger.trace { e }
                    dMetric.MarkKafkaErrInsert(1)
                }

                resp.apply {
                    status = HttpStatus.OK_200
                    addHeader("Content-Type", "application/json; charset=utf-8")
                }
            }

            /**
             * to Retrieve data from Hbase
             */

            override fun doGet(req: HttpServletRequest, resp: HttpServletResponse) {

                val key = strToLongByte(req.getParameter("key"))

                var fromDate = strToLongByte("0", true)
                if (!req.getParameter("from").isNullOrBlank())
                    fromDate = strToLongByte(req.getParameter("from"), true)

                var toDate = strToLongByte("0", true)
                if (!req.getParameter("to").isNullOrBlank())
                    toDate = strToLongByte(req.getParameter("to"), true)
//                logger.trace { "key=$key value=$value" }
//                dMetric.MarkKafkaTotal(1)
                val result = Results()

                try {
                    val salt = murmur.hashBytes(key).asBytes()

                    if (!req.getParameter("to").isNullOrBlank()) {

                        /**
                         * Since rows are sorted in reverse we have to swap
                         * the value of "from" and "to" of query parameters for scan range */

                        val fromRow = ByteBuffer.allocate(salt.size + key.size + toDate.size)
                                .put(salt).put(key).put(toDate).array()

                        val endRow = ByteBuffer.allocate(salt.size + key.size + fromDate.size)
                                .put(salt).put(key).put(fromDate).array()

                        chTable.scan(fromRow, endRow).forEach { it -> result.results[it.key] = it.value }

                    } else {
                        val rowKey = ByteBuffer.allocate(salt.size + key.size).put(salt).put(key).array()
                        chTable.scanWithPrefix(rowKey, 50).forEach { it -> result.results[it.key] = it.value }

                    }
//                    dMetric.MarkKafkaInsert(1)
                } catch (e: Exception) {
                    logger.trace { e }
//                    dMetric.MarkKafkaErrInsert(1)
//                    logger.trace { "key=$key or ts=$ts orvalue=$value is not valid." }
                }

                resp.apply {
                    status = HttpStatus.OK_200
                    addHeader("Content-Type", "application/x-binary")
                    //       writer.write(String(pfTable.get(key.toByteArray(),"cf","q")))
                    val os = resp.outputStream
                    val oos = ObjectOutputStream(os)
                    oos.writeObject(result.results)
                }
            }
        }), "/ch")


        /**k2k
         **************************************************************************************************************/
        handler.addServlet(ServletHolder(object : HttpServlet() {

            /**
             * to post binary data to kafka
             */
            override fun doPost(req: HttpServletRequest, resp: HttpServletResponse) {

                val kafkaValue = req.inputStream.readBytes(req.contentLength)
                val keyOne = strToLongByte(req.getParameter("keyOne"))
                val keyTwo = strToLongByte(req.getParameter("keyTwo"))
                var ts = strToLongByte(System.currentTimeMillis().toString(), true)

                if (!req.getParameter("ts").isNullOrBlank())
                    ts = strToLongByte(req.getParameter("ts"), true)

                val salt = murmur.hashBytes(keyOne).asBytes()
                val kafkaKey = ByteBuffer.allocate(salt.size + keyOne.size + keyTwo.size + ts.size)
                        .put(salt).put(keyOne).put(keyTwo).put(ts).array()

                dMetric.MarkKafkaTotal(1)

                try {
                    k2kKafka.put(kafkaKey, kafkaValue)
                    dMetric.MarkKafkaInsert(1)
                } catch (e: Exception) {
                    logger.trace { e }
                    dMetric.MarkKafkaErrInsert(1)
                }

                resp.apply {
                    status = HttpStatus.OK_200
                    addHeader("Content-Type", "application/json; charset=utf-8")
                }
            }

            /**
             * to Retrieve data from Hbase
             */

            override fun doGet(req: HttpServletRequest, resp: HttpServletResponse) {

                val keyOne = strToLongByte(req.getParameter("keyOne"))
                val keyTwo = strToLongByte(req.getParameter("keyTwo"))

                var fromDate = strToLongByte("0", true)
                if (!req.getParameter("from").isNullOrBlank())
                    fromDate = strToLongByte(req.getParameter("from"), true)

                var toDate = strToLongByte("0", true)
                if (!req.getParameter("to").isNullOrBlank())
                    toDate = strToLongByte(req.getParameter("to"), true)

//                logger.trace { "key=$key value=$value" }
//                dMetric.MarkKafkaTotal(1)
                val result = Results()

                try {
                    val salt = murmur.hashBytes(keyOne).asBytes()

                    if (!req.getParameter("to").isNullOrBlank()) {

                        /**
                         * Since rows are sorted in reverse we have to swap
                         * the value of "from" and "to" of query parameters for scan range */

                        val fromRow = ByteBuffer.allocate(salt.size + keyOne.size + keyTwo.size + toDate.size)
                                .put(salt).put(keyOne).put(keyTwo).put(toDate).array()

                        val endRow = ByteBuffer.allocate(salt.size + keyOne.size + keyTwo.size + fromDate.size)
                                .put(salt).put(keyOne).put(keyTwo).put(fromDate).array()

                        k2kTable.scan(fromRow, endRow).forEach { it -> result.results[it.key] = it.value }

                    } else {
                        val rowKey = ByteBuffer.allocate(salt.size + keyOne.size + keyTwo.size)
                                .put(salt).put(keyOne).put(keyTwo).array()
                        k2kTable.scanWithPrefix(rowKey, 50).forEach { it -> result.results[it.key] = it.value }

                    }
//                    dMetric.MarkKafkaInsert(1)
                } catch (e: Exception) {
                    logger.trace { e }
//                    dMetric.MarkKafkaErrInsert(1)
//                    logger.trace { "key=$key or ts=$ts orvalue=$value is not valid." }
                }

                resp.apply {
                    status = HttpStatus.OK_200
                    addHeader("Content-Type", "application/x-binary")
                    //       writer.write(String(pfTable.get(key.toByteArray(),"cf","q")))
                    val os = resp.outputStream
                    val oos = ObjectOutputStream(os)
                    oos.writeObject(result.results)
                }
            }
        }), "/k2k")


        handler.addServlet(ServletHolder(object : HttpServlet() {
            override fun doGet(req: HttpServletRequest, resp: HttpServletResponse) {

                resp.apply {
                    status = HttpStatus.OK_200
                    addHeader("Content-Type", "application/json; charset=utf-8")
                    //addHeader("Connection", "close")
                    writer.write(gson.toJson(dMetric.getInfo()))
                }
            }
        }), "/metrics")


        handler.addServlet(ServletHolder(object : HttpServlet() {
            override fun doGet(req: HttpServletRequest, resp: HttpServletResponse) {
                resp.apply {
                    status = HttpStatus.OK_200
                    addHeader("Content-Type", "text/plain; charset=utf-8")
                    addHeader("Connection", "close")
                    writer.write("Darkoob V$version is running :D")
                }
            }
        }), "/health")

        server.start()
    }
}