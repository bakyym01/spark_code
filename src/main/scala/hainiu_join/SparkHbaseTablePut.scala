package hainiu_join

import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory, Put}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object SparkHbaseTablePut {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("SparkHbaseTablePut").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val rdd: RDD[Int] = sc.parallelize(List(1,2,3,4,5,6,7,8,9,10))

    rdd.foreach(f=>{
      println(f)
      val hbaseConf = HBaseConfiguration.create()
      val connection: Connection = ConnectionFactory.createConnection(hbaseConf)
      val table = connection.getTable(TableName.valueOf("wangliang11:user_install_status"))
      val put = new Put(Bytes.toBytes(s"spark_${f}"))
      put.addColumn(Bytes.toBytes("cf1"),Bytes.toBytes("count" ),Bytes.toBytes(f))
      table.put(put)
      table.close()
      connection.close()
    }
    )
  }

}
