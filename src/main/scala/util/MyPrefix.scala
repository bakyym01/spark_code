package util

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

object MyPrefix {
  implicit def StringtoPath(path:String)=new SparkWordCount(path);
}


class SparkWordCount(val filepath:String){
  def deletePath: Unit ={
    val hadoopconf = new Configuration()
    val fileSystem = FileSystem.get(hadoopconf)
    val path = new Path(filepath)
    if (fileSystem.exists(path)){
      fileSystem.delete(path,true)
    }
  }
}
