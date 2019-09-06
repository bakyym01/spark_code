package sql

import com.mysql.jdbc.Driver
import groovy.sql.DataSet
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

object SparkMysqlSessionDataSet {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("SparkMysqlSessionDataSet").setMaster("local[*]")
    sparkConf.set("spark.sql.shuffle.partitions","1")
    val session = SparkSession.builder().config(sparkConf).getOrCreate()
    val df = session.read.format("jdbc")
      .option("driver", classOf[Driver].getName)
      .option("url", "jdbc:mysql://nn2.hadoop:3306/hainiucralwer")
      .option("dbtable", "hainiu_web_seed_internally")
      .option("user", "hainiu")
      .option("password", "12345678").load()
    df.createOrReplaceTempView("temp")
    val frame:DataFrame = session.sql("select host from temp")

    import session.implicits._
    val frame_rdd: RDD[Row] = frame.rdd
    val unit: Dataset[HainiuDataSet]= frame_rdd.map(f => {
      HainiuDataSet(s"haniu_spark_${f.mkString}")
    }).toDS()
    unit.show()
    val dataf = unit.rdd.toDF()
    dataf.show()
  }

}
case class HainiuDataSet(val host:String)
