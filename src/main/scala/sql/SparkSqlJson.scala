package sql

import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer

object SparkSqlJson {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("SparkSqlJson").setMaster("local[*]")
    sparkConf.set("spark.sql.shuffle.partitions","1")
    val sc = new SparkContext(sparkConf)
    val json_path="F:\\spark_file\\input_json\\sparl_sql_json.txt"
    val sqlC: SQLContext = new SQLContext(sc)
    val frame: DataFrame = sqlC.read.json(json_path)
    frame.show()
    frame.select(frame.col("country")).show()

    frame.select(frame.col("country"),frame.col("num").plus(1).alias("count")).show()
    frame.filter(frame.col("num").lt(3)).show()
    val df = frame.groupBy(frame.col("country")).count()

    df.printSchema()
    df.show()
    val dfrdd = df.rdd
    val value = dfrdd.mapPartitions(f => {
      val list1 = new ListBuffer[String]
      val list = f.toList
      for (next <- list) {
        val start: String = next.getString(0)
        val end: Long = next.getLong(1)
        list1 += s"${start}\t${end}"
      }
      list1.toIterator
    })

    import util.MyPrefix._

    val json_output="F:\\spark_file\\output_json\\sqljson"
    json_output.deletePath

    value.saveAsTextFile(json_output)


  }
}
