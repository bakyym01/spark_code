package sql

import com.mysql.jdbc.Driver
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object SparkMysqlSession {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("SparkMysqlSession").setMaster("local[*]")
    sparkConf.set("spark.sql.shuffle.partitions","1")
    val session = SparkSession.builder().config(sparkConf).getOrCreate()
    val frame = session.read.format("jdbc")
      .option("driver", classOf[Driver].getName)
      .option("url", "jdbc:mysql://nn2.hadoop:3306/hainiucralwer")
      .option("dbtable", "hainiu_web_seed_externally")
      .option("user", "hainiu")
      .option("password", "12345678").load()
    frame.createOrReplaceTempView("aa")
    val df = session.sql("select host from aa")
    df.show()
  }

}
