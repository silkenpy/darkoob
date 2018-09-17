package ir.rkr.darkoob.hbase

import com.typesafe.config.Config
import ir.rkr.darkoob.util.DMetric
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.HColumnDescriptor
import org.apache.hadoop.hbase.HTableDescriptor
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.*
import org.apache.hadoop.hbase.filter.PageFilter
import org.apache.hadoop.hbase.filter.PrefixFilter
import org.apache.hadoop.hbase.util.Bytes


class HbaseConnector(tableName: String, config: Config, val dMetric: DMetric) {

    val table: Table
    val connection: Connection

    init {

        val hbConfig = HBaseConfiguration.create()
        config.getObject("hbase.config").forEach({ x, y -> println("hbase config $x --> $y"); hbConfig.set(x.toString(), y.unwrapped().toString()) })
        connection = ConnectionFactory.createConnection(hbConfig)

        val admin = HBaseAdmin(hbConfig)
        val tableAvailable = admin.isTableAvailable(tableName)
        if (tableAvailable) println("pf table is already Created!")
        else {
            val tableDescriptor = HTableDescriptor(TableName.valueOf(tableName))
            tableDescriptor.addFamily(HColumnDescriptor("cf").setMaxVersions(1))
            admin.createTable(tableDescriptor)
        }
        table = connection.getTable(TableName.valueOf(tableName))
    }

    fun put(rowKey: ByteArray, value: ByteArray, CF: String, qual: String) {
        val put = Put(rowKey)
        put.addColumn(Bytes.toBytes(CF), Bytes.toBytes(qual), value)
        table.put(put)
        println("hbase put called")

    }

    fun get(rowKey: ByteArray, CF: String, Q: String, maxVersion: Int = 1): ByteArray {

        val get = Get(rowKey)
        get.maxVersions = maxVersion
        get.addColumn(Bytes.toBytes(CF), Bytes.toBytes(Q))
        return table.get(get).value()
    }

    fun scan(startKey: ByteArray, endKey: ByteArray): Map<ByteArray, ByteArray> {

        val scan = Scan(startKey, endKey)
        val result = HashMap<ByteArray, ByteArray>()
        table.getScanner(scan).forEach { it -> result[it.row] = it.value() }
        return result
    }

    fun scanWithPrefix(startWith: ByteArray, limitSize : Long = 50):  Map<ByteArray, ByteArray> {

        val filter = PrefixFilter(startWith)

        val scan = Scan()
        val limit = PageFilter(limitSize)
        scan.setFilter(limit);
        scan.filter = filter
        val result = HashMap<ByteArray, ByteArray>()
        table.getScanner(scan).forEach { it -> result[it.row] = it.value() }
        return result
    }

    fun  prefixScan(rowKeyPrefix: ByteArray): Map<ByteArray, ByteArray>{
        val filter = PrefixFilter(rowKeyPrefix)
        val scan = Scan()
        scan.setFilter(filter)

        val scanner = table.getScanner(scan)
        val results = HashMap<ByteArray,ByteArray>()
        for (result in scanner)
            results[result.row] = result.value()
        return results
    }
}