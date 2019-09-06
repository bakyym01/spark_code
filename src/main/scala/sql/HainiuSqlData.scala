package sql

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, RowFactory, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

class HainiuSqlData(private var line:String){
  def getLine(): String ={
    this.line
  }
  def setLine(line:String): Unit ={
    this.line=line
  }
}

object SparkSqlDataSchemaObject {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("SparkSqlDataSchemaObject").setMaster("local[*]")
    val sc = new SparkContext(sparkConf)
    val maprow: RDD[HainiuSqlData] = sc.textFile("F:\\spark_file\\input_json\\sparl_sql_json.txt").map(f=>new HainiuSqlData(f))

    val sQLContext = new SQLContext(sc)
    val df = sQLContext.createDataFrame(maprow,classOf[HainiuSqlData])
    df.show()
    df.collect().foreach(println(_))

  }
}
