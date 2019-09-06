package streaming

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataOutputStream, FileSystem, Path}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.{Durations, StreamingContext}

import scala.collection.mutable.ListBuffer

object SparkStreamingSocketPortHDFS {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("SparkStreamingSocketPortHDFS").setMaster("local[*]")
    val streamingContext = new StreamingContext(conf,Durations.seconds(5))
    val streaming: ReceiverInputDStream[String] = streamingContext.socketTextStream("op.hadoop",1234)
    val reducekey = streaming.flatMap(_.split(" ")).map(_->1).reduceByKey(_+_)
    reducekey.foreachRDD((r,t)=>{
     if(!r.isEmpty()){
       val value = r.coalesce(1).mapPartitionsWithIndex((partitionId, f) => {
         val list: List[(String, Int)] = f.toList
         val hadoopConf = new Configuration()
         val fs: FileSystem = FileSystem.get(hadoopConf)
         if (list.length > 0) {
           val date: String = new SimpleDateFormat("yyyyMMddHH").format(new Date)
           val file_name: String = s"hdfs://ns1/user/wangliang11/output/sparkstreaming/${partitionId}_${date}"
           val output = new Path(file_name)
           val outputStream: FSDataOutputStream =
             if (!fs.exists(output)) {
               fs.create(output)
             } else {
               fs.append(output)
             }
           list.foreach(f => {
             outputStream.write(s"${f._1}\t${f._2}".getBytes("UTF-8"))
           })
           outputStream.close()
         }
         new ListBuffer().toIterator
       })
       value.foreachPartition(f=>Unit)
     }
    }
    )
    streamingContext.start()
    streamingContext.awaitTermination()
  }
}
