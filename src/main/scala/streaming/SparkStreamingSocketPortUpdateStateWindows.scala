package streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Durations, StreamingContext}

object SparkStreamingSocketPortUpdateStateWindows {
  def main(args: Array[String]): Unit = {
    val checkpath: String = "F:/spark_output/sparkstreamingwindows_checkpoint"
    val conf = new SparkConf().setAppName("SparkStreamingSocketPortUpdateStateWindows").setMaster("local[*]")
    val streamingContext = new StreamingContext(conf, Durations.seconds(5))
    streamingContext.checkpoint(checkpath)
    val lines = streamingContext.socketTextStream("op.hadoop", 6666)
    val windowsLines: DStream[String] = lines.window(Durations.seconds(20), Durations.seconds(10))
    val mapDstream: DStream[(String, Int)] = windowsLines.flatMap(_.split(" ")).map(_ -> 1)
    val reduceMap: DStream[(String, Int)] = mapDstream.reduceByKey(_ + _)
    val updateMap = reduceMap.updateStateByKey((a: Seq[Int], b: Option[Int]) => {
      var total: Int = 0
      for (i <- a) {
        total += i
      }
      val last = if (b.isDefined) b.get else 0
      total += last
      Some(total)
    })
    updateMap.foreachRDD((r,t)=>{
      println(s"count time:${t},${r.collect().toList}")
    })
    streamingContext.start()
    streamingContext.awaitTermination()
  }
}
