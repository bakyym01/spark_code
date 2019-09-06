import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark_SecondSort {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("sparksecondarysort").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val lines: RDD[String] = sc.textFile("F:/spark_secondarysort")
    val map = lines.filter(f => {
      if (f.startsWith("(") && f.endsWith(")") && !f.equals("")) {
        true
      } else {
        false
      }
    }).map(f => {
      val list: Array[String] = f.split(",")
      val word = list(0).substring(1)
      val num = list(1).substring(0, list(1).length - 1)
      new SecondSortCom(word, num.toInt)
    })
    val result: RDD[SecondSortCom] = map.sortBy(f=>f,false)
    result.take(100).foreach(f=>println(s"${f.key} ${f.num}"))
  }
}
