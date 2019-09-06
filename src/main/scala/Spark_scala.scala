import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark_scala {
  def main(args: Array[String]): Unit = {
    import util.MyPrefix._
    val path:String="F:/spark_output";
    path.deletePath
    val sparkConf = new SparkConf().setAppName("spark-scala").setMaster("local[*]")
    val sc = new SparkContext(sparkConf)
    val textFile: RDD[String] = sc.textFile("F:/spark_input")
    val flatmap: RDD[String] = textFile.flatMap(_.split("\t"))
    val map: RDD[(String, Int)] = flatmap.map(_->1)
    val reduceMap: RDD[(String, Int)] = map.reduceByKey(_+_)
    val mapvalues: RDD[String] = reduceMap.map(x=>s"${x._1}\t${x._2}")
    val mapcache: RDD[String] = mapvalues.cache()
    val strings: Array[String] = mapcache.collect()
    println(strings.toBuffer)
    mapcache.foreach(println)
    mapcache.saveAsTextFile(path)

  }

}

