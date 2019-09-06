package sql

import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

object SparkSqlHiveql {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("SparkSqlHiveql").setMaster("local[*]")
    sparkConf.set("spark.sql.shuffle.partitions", "1")
    sparkConf.set("spark.io.compression.codec","snappy")
    val sc = new SparkContext(sparkConf)
    val orcPath = "F:/hbase_test/part-r-00000"
    val hivec = new HiveContext(sc)
    val frame = hivec.read.orc(orcPath)
    frame.printSchema()
    hivec.sql("create database if not exists hainiu")
    hivec.sql("use hainiu")
    val createTable:String=
      """
        |CREATE TABLE IF NOT EXISTS `user_install_status`(
        |  `aid` string COMMENT 'from deserializer',
        |  `pkgname` string COMMENT 'from deserializer',
        |  `uptime` bigint COMMENT 'from deserializer',
        |  `type` int COMMENT 'from deserializer',
        |  `country` string COMMENT 'from deserializer',
        |  `gpcategory` string COMMENT 'from deserializer')
        |ROW FORMAT SERDE
        |  'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
        |STORED AS INPUTFORMAT
        |  'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'
        |OUTPUTFORMAT
        |  'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
        |  TBLPROPERTIES ('orc.compress'='SNAPPY','orc.create.index'='true')
        |
      """.stripMargin
    hivec.sql(createTable)
    hivec.sql(s"load data local inpath '${orcPath}' into table user_install_status")
    val sql: String =
      """
        |select concat(country,"\t",num) as lines from
        |(select country,count(1) as num from user_install_status group by country) a
        |where a.num <100
        |
      """.stripMargin
    val dfame = hivec.sql(sql)
    val data = dfame.persist()
    data.show()
    data.write.mode(SaveMode.Overwrite).format("text").save("F:\\spark_output\\sqlhiveorc2text")

  }

}
