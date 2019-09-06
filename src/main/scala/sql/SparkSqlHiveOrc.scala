package sql

import org.apache.spark.sql.{DataFrame, Dataset, Row, SaveMode}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

object SparkSqlHiveOrc {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("SparkSqlHiveOrc").setMaster("local[*]")
    sparkConf.set("spark.sql.shuffle.partitions","1")
    val sc = new SparkContext(sparkConf)
    val orcPath="F:\\hbase_test\\000000_0_copy_1"
    val hivec = new HiveContext(sc)
    val frame: DataFrame = hivec.read.orc(orcPath)
    val df = frame.groupBy(frame.col("country")).count()
    val dfa = df.select(df.col("country"),df.col("count").alias("num"))
    val unit: Dataset[Row] = dfa.filter(dfa.col("num").lt(3000))
    val uti= unit.persist()

    uti.write.mode(SaveMode.Overwrite).format("orc").save("F:\\spark_output\\sqlhiveorc2orc")
    uti.write.mode(SaveMode.Overwrite).format("json").save("F:\\spark_output\\sqlhiveorc2json")

  }

}
