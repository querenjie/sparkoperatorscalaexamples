package sql

import java.util.Properties

import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * SparkSql jdbc mysql的例子
  */
object SparkSqlJdbcMysqlExample {
  private def createInsertData(sparkContext: SparkContext): Unit = {
    val sqlContext = new SQLContext(sparkContext)

    val data = sparkContext.parallelize(List((11, "name1"), (12, "name2"), (13, "name3"), (14, "name4"))).map(x => Row.apply(x._1, x._2))
    val schema = StructType(StructField("id", IntegerType)::StructField("name", StringType)::Nil)
    val df = sqlContext.createDataFrame(data, schema)
//    val parameter=new Properties()
//    parameter.put("user","root")
//    parameter.put("password","root")
//    parameter.put("driver","com.mysql.jdbc.Driver")
//    df.write.mode("append").jdbc("jdbc:mysql://192.168.1.20:3306/test","t_user",parameter)
    val url = "jdbc:mysql://192.168.1.20:3306/test?user=root&password=root"
    df.createJDBCTable(url, "table1", true)  //创建表并插入数据。这也会插入数据。
    df.insertIntoJDBC(url, "table1", false)    //插入数据   false为不覆盖之前数据。这是第二次插入数据
  }

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName(SparkSqlJdbcMysqlExample.getClass.getSimpleName)
    val sparkContext = new SparkContext(conf)
    SparkSqlJdbcMysqlExample.createInsertData(sparkContext)
    sparkContext.stop()
    println("aaaaaaaaaaaa")
  }
}
