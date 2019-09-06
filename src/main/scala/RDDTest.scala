import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hive.ql.io.orc.{OrcNewInputFormat, OrcStruct}
import org.apache.hadoop.io.NullWritable
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import util.{ORCFormat, ORCUtil}

import scala.collection.mutable
import scala.io.Source

object RDDTest {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("RDDTest").setMaster("local[*]")
    val sc = new SparkContext(sparkConf)
    val dict_path:String="F:/hbase_test/country_dict.dat"
    val orcPath="F:/hbase_test/part-r-00000"
    val hadoopConf = new Configuration()
    val maprdd: RDD[(NullWritable, OrcStruct)] = sc.newAPIHadoopFile(orcPath,classOf[OrcNewInputFormat],classOf[NullWritable],classOf[OrcStruct],hadoopConf)
    val strings: Iterator[String] = Source.fromFile(dict_path).getLines()
    import scala.collection.mutable._
    val tupleToB = Map[String,String]()
    strings.foreach(f=>{
      val code_country = f.split("\t")
      val code = code_country(0)
      val name = code_country(1)
      tupleToB += code->name
    })
    val broadT: Broadcast[mutable.Map[String, String]] = sc.broadcast(tupleToB)
    val hasCountry = sc.longAccumulator
    val noCountry = sc.longAccumulator

    val result: RDD[(String, String)]= maprdd.mapPartitions(f => {
      val oRCUtil = new ORCUtil
      oRCUtil.setORCtype(ORCFormat.INS_STATUS)
      val broadmap = broadT.value
      val rl: mutable.Set[(String, String)] = new HashSet[(String, String)]()
      f.foreach(ff => {
        oRCUtil.setRecord(ff._2)
        val code: String = oRCUtil.getData("country")
        val aid = oRCUtil.getData("aid")
        val country: String = broadmap.getOrElse(code, "")
        if (!country.equals("") && !"".equals(aid)) {
          rl += ((country, aid))
          hasCountry.add(1L)
        } else {
          noCountry.add(1L)
        }
      })
      rl.toIterator
    })
    val final_re: RDD[(String, Int)] = result.mapValues(f=>HashSet(f)).reduceByKey(_++_).mapValues(f=>f.size)
    final_re.collect().foreach(println(_))
    
  }
}
