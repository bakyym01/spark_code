package hainiu_join

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{ConnectionFactory, Result, Scan}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.{TableInputFormat, TableMapReduceUtil}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object ScanTest {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("ScanTest").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val scan = new Scan()
    scan.addFamily(Bytes.toBytes("cf1"))
    scan.setCaching(1000)
    scan.setCacheBlocks(false)
    scan.setStartRow(Bytes.toBytes("spark_batch"))
    scan.setStopRow(Bytes.toBytes("spark_batch_z"))

    val hbaseConf = HBaseConfiguration.create()
    hbaseConf.set(TableInputFormat.INPUT_TABLE,"wangliang11:user_install_status")
    hbaseConf.set(TableInputFormat.SCAN,TableMapReduceUtil.convertScanToString(scan))
    val unit: RDD[(ImmutableBytesWritable, Result)] = sc.newAPIHadoopRDD(hbaseConf,classOf[TableInputFormat],classOf[ImmutableBytesWritable],classOf[Result])
    unit.foreach(f=>{
      val cf = Bytes.toBytes("cf1")
      val c = Bytes.toBytes("count")
      val key = Bytes.toString(f._1.get())
      val value = Bytes.toInt(f._2.getValue(cf,c))
      println(s"rowkey:${key},value:${value}")
    })

//    val maprdd: RDD[(ImmutableBytesWritable, Result)] = sc.newAPIHadoopRDD(hbaseConf,classOf[TableInputFormat],classOf[ImmutableBytesWritable],classOf[Result])
//
//    maprdd.foreach(f=>{
//
//
//
//    })




  }

}
