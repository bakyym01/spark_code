package streaming

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Durations, StreamingContext}

object SparkStreamingSocketFileCogroup {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("SparkStreamingSocketFileCogroup").setMaster("local[*]")
    conf.set("spark.streaming.fileStream.minRememberDuration", "2592000s")
    val streamingContext = new StreamingContext(conf, Durations.seconds(5))
    val lines: ReceiverInputDStream[String] = streamingContext.socketTextStream("op.hadoop", 6666)
    val countryCount: DStream[(String, Int)] = lines.flatMap(_.split(" ")).map(_ -> 1).reduceByKey(_ + _)
    val inputPath: String = "F:/spark_file/input_sparkstreamingfileasocket"
    val hadoopConf = new Configuration()
    val fileStream = streamingContext.fileStream[LongWritable, Text, TextInputFormat](inputPath, (filter: Path) => {
      filter.getName.endsWith(".txt")
    }, false, hadoopConf)
    val fileDM: DStream[(String, String)] = fileStream.map(f => {
      val strings = f._2.toString.split("\t")
      (strings(0), strings(1))
    })
    countryCount.join(fileDM).foreachRDD(f=>{


    })
//    countryCount.cogroup(fileDM).foreachRDD((f,t)=>{
//      f.foreach(ff=>{
//       println(s"count time:${t},country code:${ff._1},country count:${ff._2._1},country name:${ff._2._2}")
//      }
//      )
//    })
    streamingContext.start()
    streamingContext.awaitTermination()

  }
}
