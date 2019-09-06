package hainiu_join

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{ConnectionFactory, Mutation, Put}
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.TableName
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.mapreduce.{MRConfig, MRJobConfig}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object TablePutFormat {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("SparkHbaseTablePut").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val rdd: RDD[Int] = sc.parallelize(List(1, 2, 3, 4, 5, 6, 7, 8, 9, 10),1)
    val hbaseConf = HBaseConfiguration.create()
    hbaseConf.set(TableOutputFormat.OUTPUT_TABLE,"wangliang11:user_install_status")
    hbaseConf.set(MRJobConfig.OUTPUT_FORMAT_CLASS_ATTR,classOf[TableOutputFormat[NullWritable]].getName)
    hbaseConf.set(MRJobConfig.OUTPUT_KEY_CLASS,classOf[NullWritable].getName)
    hbaseConf.set(MRJobConfig.OUTPUT_VALUE_CLASS,classOf[Mutation].getName)
    val connection = ConnectionFactory.createConnection(hbaseConf)

    val unit: RDD[(NullWritable, Put)] = rdd.map(f => {
      val put = new Put(Bytes.toBytes(s"spark_tableformat_${f}"))
      put.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("count"), Bytes.toBytes(f))
      (NullWritable.get(), put)
    })
    unit.saveAsNewAPIHadoopDataset(hbaseConf)

  }

}
