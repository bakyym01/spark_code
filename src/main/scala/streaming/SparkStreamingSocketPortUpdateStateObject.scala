package streaming

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Durations, StreamingContext}

case class StreamingStatusValue(var value:Int,var isUpdate:Boolean=false)
object SparkStreamingSocketPortUpdateStateObject {
  def main(args: Array[String]): Unit = {
    val checkpath="F:/spark_output/sparkstreaming_checkpoint"
    val src: StreamingContext = StreamingContext.getOrCreate(checkpath, () => {
      val conf = new SparkConf().setAppName("SparkStreamingSocketPortUpdateStateObject").setMaster("local[*]")
      val streamingContext = new StreamingContext(conf, Durations.seconds(5))
      streamingContext.checkpoint(checkpath)
      val lines = streamingContext.socketTextStream("op.hadoop", 6666)
      val maplines: DStream[(String, StreamingStatusValue)] = lines.flatMap(_.split(" ")).map(_ -> StreamingStatusValue(1))
      val combineValue: DStream[(String, StreamingStatusValue)] = maplines.reduceByKey((a, b) => StreamingStatusValue(a.value + b.value))
      val updateValue: DStream[(String, StreamingStatusValue)] = combineValue.updateStateByKey((a: Seq[StreamingStatusValue], b: Option[StreamingStatusValue]) => {
        var total: Int = 0
        for (i <- a) {
          total += i.value
        }
        val last: StreamingStatusValue = if (b.isDefined) b.get else StreamingStatusValue(0)
        if (a.size != 0) {
          last.isUpdate = true
        } else {
          last.isUpdate = false
        }
        last.value=total+last.value
        Some(last)
      })
      updateValue.foreachRDD((r, t) => {
        val filterValue: RDD[(String, StreamingStatusValue)] = r.filter(_._2.isUpdate)
        println(s"count time:${t},${filterValue.collect().toList}")
      })

      streamingContext
    })
    src.start()
    src.awaitTermination()
  }

}
