package streaming




import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Durations, StreamingContext}

object SparkStreamingFile {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("SparkStreamingFile").setMaster("local[2]")
    conf.set("spark.streaming.fileStream.minRememberDuration","2592000s")
    val streamingContext = new StreamingContext(conf,Durations.seconds(5))
    streamingContext.checkpoint("F:/spark_output/sparkstreamingfile_checkpoint")
    val inputPath:String="F:/spark_file/input_sparkstreamingfile"
    val hadoopConf = new Configuration()
    val fileStream: InputDStream[(LongWritable, Text)] = streamingContext.fileStream[LongWritable, Text, TextInputFormat](inputPath, (f: Path) => {
      f.getName.endsWith(".txt")
    }, false, hadoopConf)
    val mapDstream: DStream[(String, Int)] = fileStream.flatMap(_._2.toString.split("\t")).map(_->1)
    val reduceDm: DStream[(String, Int)] = mapDstream.reduceByKey(_+_)
    val updateDm = reduceDm.updateStateByKey((a: Seq[Int], b: Option[Int]) => {
      var total: Int = 100
      for (i <- a) {
        total += i
      }
      val last = if (b.isDefined) b.get else 0
      total += last
      Some(total)
    })
    updateDm.foreachRDD((r,t)=>{
      println(s"count time:${t},${r.collect().toList}")
    })
    streamingContext.start()
    streamingContext.awaitTermination()
  }
}
