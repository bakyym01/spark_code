package streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Durations, StreamingContext}

object SparkstreamingUpdateStateKey {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("SparkstreamingUpdateStateKey").setMaster("local[*]")
    val streamingContext = new StreamingContext(conf,Durations.seconds(5))
    streamingContext.checkpoint("F:/spark_output/sparkstreaming_checkpoint")
    val content: ReceiverInputDStream[String] = streamingContext.socketTextStream("localhost",6666)
    val temp = content.flatMap(_.split(" ")).map(_->1)
    val result = temp.reduceByKey(_+_)
    val updateValue: DStream[(String, Int)] = result.updateStateByKey((list: Seq[Int], last: Option[Int]) => {
      var total: Int = 0
      for (i <- list) {
        total += i
      }
      val sum = if (last.isDefined) last.get else 0
      val now =total+sum
      Some(now)
    })
    updateValue.foreachRDD((f,t)=>{
      println(s"count time ${t},${f.collect().toList}")
    })
    streamingContext.start()
    streamingContext.awaitTermination()

  }
}
