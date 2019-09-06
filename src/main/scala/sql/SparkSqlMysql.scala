package sql

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

object SparkSqlMysql {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("SparkSqlMysql").setMaster("local[*]")
    sparkConf.set("spark.sql.shuffle.partitions","1")
    val sc = new SparkContext(sparkConf)
    val sQLContext = new SQLContext(sc)
    val frame = sQLContext.jdbc("jdbc:mysql://nn2.hadoop:3306/hainiucralwer?user=hainiu&password=12345678","hainiu_web_seed_internally")
    val df1 = sQLContext.jdbc("jdbc:mysql://nn2.hadoop:3306/hainiucralwer?user=hainiu&password=12345678","hainiu_web_seed_externally")
    frame.createOrReplaceTempView("aa")
    df1.createOrReplaceTempView("bb")

    val unit = sQLContext.sql("select * from aa inner join bb where aa.md5=bb.md5 limit 100")
    unit.show()


  }

}
