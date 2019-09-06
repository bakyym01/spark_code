import org.apache.spark.broadcast.Broadcast
import org.apache.spark.{SparkConf, SparkContext}

object Spark_broadcast {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("sparkbroadcastdemo").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val arr=Array(1,2,3,4,5)
    val par = sc.parallelize(arr,5)
    val broad: Broadcast[Array[Int]] = sc.broadcast(arr)
    val acc = sc.accumulator(0)

    var reduceResult= par.reduce((a,b)=>{
      val ints: Array[Int] = broad.value
      println(s"broad:${ints.toList}")
      println(s"arr:${arr.toList}")
      acc.add(1)
      a+b
    })
    println(reduceResult)
    println(acc)

  }

}
