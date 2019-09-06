package driver
import gameSimilarity.GameAppSimilarity
import hainiu_join.{MapJoin, SparkHBaseBulkLoad}
import org.apache.hadoop.util.ProgramDriver

object driver {
  def main(args: Array[String]): Unit = {
    val driver = new ProgramDriver
    driver.addClass("hainiu_join.MapJoin",classOf[MapJoin],"MapJoin任务")
    driver.addClass("sparkload",classOf[SparkHBaseBulkLoad],"sparkloadtohbase上传")
    driver.addClass("gamesimilarity",classOf[GameAppSimilarity],"相似度推荐")
    driver.run(args)
  }
}
