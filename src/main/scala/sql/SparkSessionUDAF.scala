package sql

import com.mysql.jdbc.Driver
import org.apache.spark.SparkConf
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._


object SparkSessionUDAF {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("SparkSessionUDAF").setMaster("local[*]")
    sparkConf.set("spark.sql.shuffle.partitions","1")
    val session = SparkSession.builder().config(sparkConf).getOrCreate()
    val df = session.read.format("jdbc")
      .option("driver", classOf[Driver].getName)
      .option("url", "jdbc:mysql://nn2.hadoop:3306/hainiucralwer")
      .option("dbtable", "hainiu_web_seed_internally")
      .option("user", "hainiu")
      .option("password", "12345678").load()
    session.udf.register("aveupdate",AveUDAF)
    df.createOrReplaceTempView("temp")
    session.sql("select aveupdate(create_hour) from temp").show()

  }
}

object AveUDAF extends UserDefinedAggregateFunction{
  override def inputSchema: StructType = StructType(StructField("input",LongType)::Nil)

  override def bufferSchema: StructType = StructType(StructField("sum",LongType)::StructField("count",LongType)::Nil)

  override def dataType: DataType = DoubleType

  override def deterministic: Boolean = true

  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = 0L
    buffer(1) = 0L
  }

  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    buffer(0) = buffer.getLong(0) + input.getLong(0)
    buffer(1) = buffer.getLong(1)+1L
  }

  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1(0) = buffer1.getLong(0)+buffer2.getLong(0)
    buffer1(1) = buffer1.getLong(1)+buffer2.getLong(1)
  }

  override def evaluate(buffer: Row): Any = {
    buffer.getLong(0).toDouble/buffer.getLong(1)
  }
}
