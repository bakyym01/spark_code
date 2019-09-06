package sql

import java.sql.{Connection, DriverManager, ResultSet, Statement}

object SparkServeJDBC {
  def main(args: Array[String]): Unit = {
    var connection:Connection=null;
    try{
      classOf[org.apache.hive.jdbc.HiveDriver]
      connection = DriverManager.getConnection("jdbc:hive2://op.hadoop:20000/hainiu","","")
      val statement: Statement = connection.createStatement()
      val sql:String =
        """
          |select count(1) as c
          |from
          |( select count(1) from user_install_status group by aid) a
          |
        """.stripMargin

      val set: ResultSet = statement.executeQuery(sql)
      while(set.next()){
        println(set.getLong("c"))
      }
    }catch{
      case e:Exception=>e.printStackTrace()
    }finally {
      if(connection!=null){
        connection.close()
      }

    }
  }

}
