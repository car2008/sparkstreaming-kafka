package mytest.test1
import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd._
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat
import org.apache.spark.SparkConf
import org.apache.spark.sql._
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{ Seconds, StreamingContext }
import org.apache.hadoop.hbase.client.{ Mutation, Put }
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.mapreduce.OutputFormat
import org.apache.hadoop.hbase.client.{HTable, Put}
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
/**
 * @author ${user.name}
 */
object App2 {

  def main(args: Array[String]) {
    println("begin to write data to hbase...")
    val conf = new SparkConf().setAppName("use the foreachRDD write data to hbase").setMaster("local[2]")
    /*var rank = 0
    val ssc = new StreamingContext(conf, Seconds(10))
    val lines = KafkaUtils.createStream(ssc, "localhost:2181", "group4", Map("test3" -> 1)).map(_._2)*/
    //val lines = KafkaUtils.createStream(ssc, zk, "group4", topics).map(_._2)
    //lines.map{x => (x._2,1)}.reduceByKey {_ + _}.print
    //val linearrays = lines.map(line => line._2.split('\t')).map(s => (s(0).toString, s(1).toDouble, s(2).toDouble, s(3).toDouble))

    //kafka topic
    //val topics = List(("test3", 1)).toMap
    //zookeeper
    //val zk = "127.0.0.1"
    //val conf = new SparkConf() setMaster "local[2]" setAppName "SparkStreamingETL"
    //create streaming context
    val ssc = new StreamingContext(conf, Seconds(10))
    //get every lines from kafka
    //val lines = KafkaUtils.createStream(ssc, zk, "group4", topics).map(_._2)
    val lines = KafkaUtils.createStream(ssc, "localhost:2181", "group4", Map("test3" -> 1)).map(_._2)
    //get spark context
    val sc = ssc.sparkContext
    //get sql context
    val sqlContext = new SQLContext(sc)
    //process every rdd AND save as HTable
    lines.foreachRDD(rdd => {
      //case class implicits
      import sqlContext.implicits._
      //filter empty rdd
      if (!rdd.isEmpty) {
        //register a temp table
        rdd.map(_.split("\t")).map(p => Persion(p(0), p(1).trim , p(2).trim.toInt, p(3).trim.toDouble)).toDF.registerTempTable("oldDriver")
        //use spark SQL
        val rs = sqlContext.sql("select name,gender,age,driverAge from oldDriver")
        //create hbase conf
        val hconf = HBaseConfiguration.create()
        hconf.set("hbase.zookeeper.quorum", "yarn001:2181")
        hconf.set("hbase.zookeeper.property.clientPort", "2181")
        hconf.set("hbase.defaults.for.version.skip", "true")
        //hconf.set(TableOutputFormat.OUTPUT_TABLE, "obd_pv")
        //hconf.setClass("mapreduce.job.outputformat.class", classOf[TableOutputFormat[String]], classOf[OutputFormat[String, Mutation]])
        //val jobConf = new JobConf(hconf)
        //convert every line to hbase lines
        //val StatTable = new HTable(hconf, TableName.valueOf("obd_pv"))
        //StatTable.setAutoFlush(false, false)
        val StatTable = new HTable(hconf, TableName.valueOf("obd_pv"))
        StatTable.setAutoFlush(false, false)
        rs.map(_.getValuesMap[Any](List("name", "gender","age","driverAge"))).collect().foreach({ map =>
          val name = map.get("name").getOrElse("")
          val gender = map.get("gender").getOrElse("")
          val age = map.get("age").getOrElse("")
          val driverAge = map.get("driverAge").getOrElse("")
          val put = new Put(Bytes.toBytes(name.toString+System.currentTimeMillis()))
          put.add("colfam1".getBytes, "gender".getBytes, Bytes.toBytes(gender.toString))
          put.add("colfam1".getBytes, "age".getBytes, Bytes.toBytes(age.toString))
          put.add("colfam1".getBytes, "driverAge".getBytes, Bytes.toBytes(driverAge.toString))
          //写入数据缓存
          StatTable.setWriteBufferSize(3*1024*1024)
          StatTable.put(put)
        })
        StatTable.flushCommits()
        
        /*rs.rdd.map(line => (System.currentTimeMillis(), line(1))).map(line => {
          //create hbase put
          val put = new Put(Bytes.toBytes(line._1))
          //add column
          println(line._1)
          println(line._2)
          put.add(Bytes.toBytes("colfam1"), Bytes.toBytes("pv"), Bytes.toBytes(line._2.toString))
          //retuen type
          (new ImmutableBytesWritable, put)
        }).saveAsNewAPIHadoopDataset(jobConf) //save as HTable
*/      }
    })
    //streaming start
    ssc start ()
    ssc awaitTermination ()
  }
  //the entity of persion for SparkSQL
  case class Persion(name: String,gender: String, age: Int, driverAge: Double)
}
