package streaming

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Durations, StreamingContext}

object SparkStreamingSocketPort {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("SparkStreamingSocketPort").setMaster("local[*]")
    val context = new StreamingContext(sparkConf,Durations.seconds(5))
    val lines: ReceiverInputDStream[String] = context.socketTextStream("localhost",6666)
//    val dstream: DStream[(String, Int)] = lines.flatMap(f=>f.split(" ")).map(f=>(f,1))
//    val value: DStream[(String, Int)] = dstream.reduceByKey(_+_)
//    value.foreachRDD((f,t)=>{
//      println(s"count:${f.collect().toList},${t}")
//    })
//    lines.foreachRDD((f,t)=>{
//      val temp: RDD[(String, Int)] = f.flatMap(_.split(" ")).map((_,1))
//      val count: RDD[(String, Int)] = temp.reduceByKey(_+_)
//      println(s"count time ${t},${count.collect().toList}")
//    })
    val value = lines.transform((r, t) => {
      val temp = r.flatMap(_.split(" ")).map(_ -> 1)
      val reduceresult = temp.reduceByKey(_ + _)

      reduceresult
    })
    value.foreachRDD((r,t)=>{
      println(s"count time ${t},${r.collect().toBuffer}")
    })

    context.start()
    context.awaitTermination()
  }
}
