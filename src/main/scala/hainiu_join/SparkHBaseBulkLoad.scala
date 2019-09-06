package hainiu_join


import org.apache.hadoop.fs.Path
import org.apache.hadoop.hbase.client.{ConnectionFactory, HTable}
import org.apache.hadoop.hbase.{HBaseConfiguration, KeyValue, TableName}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.{HFileOutputFormat2, LoadIncrementalHFiles}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.rdd.RDD
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer

class SparkHBaseBulkLoad
object SparkHBaseBulkLoad {
  def main(args: Array[String]): Unit = {
    import util.MyPrefix._
//    val outputPath:String="F:/spark_output/spark_hfile"
    val outputPath = args(1)
    outputPath.deletePath
    val sparkConf = new SparkConf().setAppName("SparkHBaseBulkLoad").setMaster("local[*]")
    sparkConf.set("spark.serializer",classOf[KryoSerializer].getName)
    val sc = new SparkContext(sparkConf)
//    val orcPath="F:/hbase_test/part-r-00000"
    val orcPath = args(0)
    val hivec = new HiveContext(sc)
    val frame: DataFrame = hivec.read.orc(orcPath)
    val rddfram: RDD[Row] = frame.limit(100).rdd

    val resultRdd: RDD[(ImmutableBytesWritable, KeyValue)] = rddfram.mapPartitions(f => {
      val list1 = new ListBuffer[(ImmutableBytesWritable, KeyValue)]
      val list: List[Row] = f.toList
      for (next <- list) {
        val rowkey = new ImmutableBytesWritable()
        val country = next.getString(1)
        rowkey.set(Bytes.toBytes(s"spark_load_${country}"))
        val value = new KeyValue(rowkey.get(), Bytes.toBytes("cf1"), Bytes.toBytes("count"), Bytes.toBytes(4))
        list1 += ((rowkey, value))
      }
      list1.toIterator
    }).sortByKey()

    val hbaseConf = HBaseConfiguration.create()
    val job = Job.getInstance()
    val connection = ConnectionFactory.createConnection(hbaseConf)
    val table: HTable = connection.getTable(TableName.valueOf("wangliang11:user_install_status")).asInstanceOf[HTable]
    HFileOutputFormat2.configureIncrementalLoad(job,table.getTableDescriptor,table.getRegionLocator)
    resultRdd.saveAsNewAPIHadoopFile(outputPath,classOf[ImmutableBytesWritable],classOf[KeyValue],classOf[HFileOutputFormat2],hbaseConf)

    val loader = new LoadIncrementalHFiles(hbaseConf)
    val admin = connection.getAdmin
    loader.doBulkLoad(new Path(outputPath),admin,table,table.getRegionLocator())
  }

}
