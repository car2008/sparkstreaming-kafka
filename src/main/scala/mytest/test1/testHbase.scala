package mytest.test1
import kafka.serializer.StringDecoder
import net.sf.json.JSONObject
import org.apache.hadoop.hbase.client.{HTable, Put}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
object testHbase {
  def main(args: Array[String]) {
    println("test to write data to hbase...")
    val tableName = "obd_pv"
    val hbaseConf = HBaseConfiguration.create()
    hbaseConf.set("hbase.zookeeper.quorum", "yarn001:2181")
    hbaseConf.set("hbase.zookeeper.property.clientPort", "2181")
    hbaseConf.set("hbase.defaults.for.version.skip", "true")
    //用户ID
    val uid = "www.baidu.com"
    //点击次数
    val click = "chaxunduocide yiyi"
    //组装数据
    val put = new Put(Bytes.toBytes(uid))
    put.add("colfam1".getBytes, "ClickStat".getBytes, Bytes.toBytes(click))
    val StatTable = new HTable(hbaseConf, TableName.valueOf(tableName))
    StatTable.setAutoFlush(false, false)
    //写入数据缓存
    StatTable.setWriteBufferSize(3*1024*1024)
    StatTable.put(put)
    //提交
    StatTable.flushCommits()
  }
}