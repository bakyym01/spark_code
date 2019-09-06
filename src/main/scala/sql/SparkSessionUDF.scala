package sql

import com.mysql.jdbc.Driver
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object SparkSessionUDF {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("SparkSessionUDF").setMaster("local[*]")
    sparkConf.set("spark.sql.shuffle.partitions","1")
    val session = SparkSession.builder().config(sparkConf).getOrCreate()
    val df = session.read.format("jdbc")
      .option("driver", classOf[Driver].getName)
      .option("url", "jdbc:mysql://nn2.hadoop:3306/hainiucralwer")
      .option("dbtable", "hainiu_web_seed_internally")
      .option("user", "hainiu")
      .option("password", "12345678").load()
    df.createOrReplaceTempView("temp")
    session.udf.register("hainiu",(a:String)=>{s"modified_${a}"})
    session.sql("select hainiu(host) from temp").show()

  }
}
