package ir.rkr.darkoob.hbase

import com.typesafe.config.Config
import ir.rkr.darkoob.util.DMetric
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.Connection
import org.apache.hadoop.hbase.client.ConnectionFactory
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.client.Table
import org.apache.hadoop.hbase.util.Bytes


class HbaseConnector(val tableName: String, config: Config, val dMetric: DMetric) {

    val table: Table
    val connection: Connection

    //  val put: Put

    init {

        val hbConfig = HBaseConfiguration.create()
        config.getObject("hbase.config").forEach({ x, y -> println("hbase config $x --> $y"); hbConfig.set(x.toString(), y.unwrapped().toString()) })
        connection = ConnectionFactory.createConnection(hbConfig)
        table = connection.getTable(TableName.valueOf(tableName))
    }

    fun put(CF: String, qual: String, rowKey: ByteArray, value: ByteArray) {

        val put = Put(rowKey)
        put.addColumn(Bytes.toBytes(CF) , Bytes.toBytes(qual), value)
        table.put(put)

    }
//        put = Put("test".toByteArray())
//
//
//
//        put.addColumn(Bytes.toBytes("personal"),
//                Bytes.toBytes("name"), Bytes.toBytes("milux"))
//        table.put(put)

}