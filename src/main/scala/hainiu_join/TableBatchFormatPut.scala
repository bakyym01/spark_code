package hainiu_join

import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.{ConnectionFactory, Mutation, Put}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapreduce.MRJobConfig
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.hive.HiveContext

import scala.collection.mutable.ListBuffer

object TableBatchFormatPut {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("SparkHbaseTablePut").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val orcPath = "F:/hbase_test/part-r-00000"
    val hivec = new HiveContext(sc)
    val df = hivec.read.orc(orcPath)
    df.createOrReplaceTempView("user_install_status")
    val dataFrame = hivec.sql("select pkgname,count(1) from user_install_status group by pkgname")
    val datardd: RDD[Row] = dataFrame.rdd
    val rep: RDD[Row] = datardd.repartition(100)
    val acc = sc.longAccumulator

    val maprdd: RDD[(ImmutableBytesWritable, Put)] = rep.mapPartitions(f => {
      val list: List[Row] = f.toList
      acc.add(1L)
      val listb = new ListBuffer[(ImmutableBytesWritable, Put)]
      for (next <- list) {
        val key = new ImmutableBytesWritable
        val put = new Put(Bytes.toBytes(s"tableBatchFormatPut_${next.getString(0)}"))
        put.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("count"), Bytes.toBytes(next.getLong(1)))
        key.set(put.getRow)
        listb += ((key, put))
      }
      listb.toIterator
    })

    val hbaseConf = HBaseConfiguration.create()
    hbaseConf.set(TableOutputFormat.OUTPUT_TABLE,"wangliang11:user_install_status")
    hbaseConf.set(MRJobConfig.OUTPUT_FORMAT_CLASS_ATTR,
      classOf[TableOutputFormat[ImmutableBytesWritable]].getName)
    hbaseConf.set("mapreduce.job.output.key.class", classOf[ImmutableBytesWritable].getName)
    hbaseConf.set("mapreduce.job.output.value.class", classOf[Put].getName)
    val mapreduce: RDD[(ImmutableBytesWritable, Put)] = maprdd.coalesce(5)
    mapreduce.saveAsNewAPIHadoopDataset(hbaseConf)

  }

}
