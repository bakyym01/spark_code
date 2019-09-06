package sql

import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}
import org.apache.spark.{SparkConf, SparkContext}

object SparkSqlDataShema {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("SparkSqlDataShema").setMaster("local[*]")
    sparkConf.set("spark.sql.shuffle.partitions","1")
    val sc = new SparkContext(sparkConf)
    val sQLContext = new SQLContext(sc)
    val inpath="F:\\spark_file\\input_json\\sparl_sql_json.txt"
    val ds: RDD[Row]  = sc.textFile(inpath).map(f=>RowFactory.create(f))
    val lines: StructField = DataTypes.createStructField("lines",types.StringType,true)
    val structType: StructType = StructType(lines :: Nil)

    val df = sQLContext.createDataFrame(ds,structType)
    df.show()
    val dsr: Dataset[Row] = df.filter(df.col("lines").like("%gameloft%"))
    dsr.show()
    val dfrdd = dsr.rdd
    dfrdd.collect().foreach(println)


  }
}
