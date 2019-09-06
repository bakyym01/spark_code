package streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Durations, StreamingContext}

import scala.collection.mutable.ListBuffer

object SparkStreamingMuiReceiver {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("SparkStreamingMuiReceiver").setMaster("local[*]")
    val streamingContext = new StreamingContext(conf,Durations.seconds(5))
    val socket1: ReceiverInputDStream[String] = streamingContext.socketTextStream("op.hadoop",1234)
    val socket2: ReceiverInputDStream[String] = streamingContext.socketTextStream("op.hadoop",5678)
    val buffer = ListBuffer[ReceiverInputDStream[String]](socket1,socket2)
    val sDM: DStream[String] = streamingContext.union(buffer)
    sDM.map(_->1).foreachRDD((r,t)=>{
      r.foreach(println(_))
    })
    streamingContext.start()
    streamingContext.awaitTermination()
  }

}
