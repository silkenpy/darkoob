package ir.rkr.darkoob.hbase

import com.typesafe.config.Config
import ir.rkr.darkoob.util.DMetric
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.HColumnDescriptor
import org.apache.hadoop.hbase.HTableDescriptor
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.*
import org.apache.hadoop.hbase.util.Bytes


class HbaseConnector(val tableName: String, config: Config, val dMetric: DMetric) {

    val table: Table
    val connection: Connection

    init {

        val hbConfig = HBaseConfiguration.create()
        config.getObject("hbase.config").forEach({ x, y -> println("hbase config $x --> $y"); hbConfig.set(x.toString(), y.unwrapped().toString()) })
        connection = ConnectionFactory.createConnection(hbConfig)

        val admin = HBaseAdmin(hbConfig)
        val tableAvailable = admin.isTableAvailable("pf")
        if (tableAvailable) println("pf table is already Created!")
        else {
            val tableDescriptor = HTableDescriptor(TableName.valueOf("pf"))
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
        val result = HashMap<ByteArray,ByteArray>()
        table.getScanner(scan).forEach{it -> result[it.row] = it.value()}
        return result
    }
}