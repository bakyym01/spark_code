package hainiu_join


import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hive.ql.io.orc.{OrcNewInputFormat, OrcNewOutputFormat, OrcStruct}
import org.apache.hadoop.io.{NullWritable, Writable}
import org.apache.orc.OrcProto.CompressionKind
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import util.{ORCFormat, ORCUtil}

import scala.collection.mutable
import scala.io.Source

class MapJoin
object MapJoin {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("mapjoin").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val orcpath="F:\\hbase_test\\000000_0"
    val country_dict="F:\\hbase_test\\country_dict.dat"
    val hadoopConf = new Configuration()
    val mapdrdd: RDD[(NullWritable, OrcStruct)] = sc.newAPIHadoopFile(orcpath,classOf[OrcNewInputFormat],classOf[NullWritable],classOf[OrcStruct],hadoopConf)
    import scala.collection.mutable._
    val map = Map[String,String]()
    val list = Source.fromFile(country_dict).getLines().toList
    list.foreach(f=>{
      val strings = f.split("\t")
      val code = strings(0)
      val countryName = strings(1)
      map += ((code,countryName))
    })
    val value: Broadcast[mutable.Map[String, String]] = sc.broadcast(map)
    val noCountry = sc.longAccumulator
    val hasCountry = sc.longAccumulator


    val unit = mapdrdd.mapPartitions(f => {
      val broadcastT = value.value
      val listBuffer = ListBuffer[String]()
      val oRCUtil = new ORCUtil
      oRCUtil.setORCtype(ORCFormat.INS_STATUS)

        f.foreach(ff => {
          println(ff)
        oRCUtil.setRecord(ff._2)
        val code = oRCUtil.getData("country")
        val countryname = broadcastT.getOrElse(code, "")
        if (!countryname.equals("")) {
          hasCountry.add(1L)
          listBuffer += s"${code}\t${countryname}"
        } else {
          noCountry.add(1L)
        }
      }
      )
      listBuffer.toIterator
    })

    val saveOrc: RDD[(NullWritable, Writable)]= unit.mapPartitions(f => {
      val util = new ORCUtil
      util.setORCWriteType("struct<code:string,country_name:string>")
      val listBuffer = ListBuffer[(NullWritable, Writable)]()
      f.foreach(
        ff => {
          val strings = ff.split("\t")
          util.addAttr(strings(0), strings(1))
          listBuffer += ((NullWritable.get(), util.serialize()))
        }
      )
      listBuffer.toIterator
    }
    )


    import util.MyPrefix._

    hadoopConf.set("orc.create.index","true")
    hadoopConf.set("hive.exec.orc.default.compress",CompressionKind.SNAPPY.name())
    val outpath = "F:/output_orc"
    outpath.deletePath

    saveOrc.saveAsNewAPIHadoopFile(outpath,classOf[NullWritable],classOf[Writable],classOf[OrcNewOutputFormat],hadoopConf)
    println(hasCountry)
    println(noCountry)
  }
}



