package sql


import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

object SparkHiveText {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("SparkHiveText").setMaster("local[*]")
    sparkConf.set("spark.sql.shuffle.partitions", "1")
    val sc = new SparkContext(sparkConf)
    val orcPath = "F:\\hbase_test\\part-r-00000"
    val hivec = new HiveContext(sc)
    val df = hivec.read.orc(orcPath)
    df.printSchema()
    df.createOrReplaceTempView("user_install")
    val sql: String =
      """
        |select concat(country,"\t",num) as lines from
        |(select country,count(1) as num from user_install group by country) a
        |where a.num <100
        |
      """.stripMargin
    val dfame = hivec.sql(sql)
    val data = dfame.persist()
    data.write.mode(SaveMode.Overwrite).format("text").save("F:\\spark_output\\sqlhiveorc2text")
  }
}
