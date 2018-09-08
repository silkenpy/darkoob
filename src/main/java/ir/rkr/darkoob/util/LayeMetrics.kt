package ir.rkr.darkoob.util

import com.codahale.metrics.Gauge
import com.codahale.metrics.Meter
import com.codahale.metrics.MetricRegistry
import java.util.function.Supplier


data class MeterPojo(val count: Long,
                     val rate: Double,
                     val oneMinuteRate: Double,
                     val fiveMinuteRate: Double,
                     val fifteenMinuteRate: Double)

data class ServerInfo(val gauges: Map<String, Any>, val meters: Map<String, MeterPojo>)

class LayeMetrics {

    val metricRegistry = MetricRegistry()

    val KafkaTotal = metricRegistry.meter("KafkaTotal")
    val KafkaInsert = metricRegistry.meter("KafkaInsert")
    val KafkaErrInsert = metricRegistry.meter("KafkaErrInsert")

    fun MarkKafkaTotal(l: Long = 1) = KafkaTotal.mark(l)
    fun MarkKafkaInsert(l: Long = 1) = KafkaInsert.mark(l)
    fun MarkKafkaErrInsert(l: Long = 1) = KafkaErrInsert.mark(l)


    fun <T> addGauge(name: String, supplier: Supplier<T>) = metricRegistry.register(name, Gauge<T> { supplier.get() })

    private fun sortMetersByCount(meters: Map<String, Meter>) =
            meters.toList().sortedBy { it.second.count }.reversed()
                    .map { Pair(it.first, it.second.toPojo()) }.toMap()

    private fun Meter.toPojo() = MeterPojo(count, meanRate, oneMinuteRate, fiveMinuteRate, fifteenMinuteRate)

    fun getInfo() = ServerInfo(metricRegistry.gauges.mapValues { it.value.value },
            sortMetersByCount(metricRegistry.meters))


}

