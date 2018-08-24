package sql

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SQLContext}

/**
  * UDAF的例子。UDAF的特点是传入多个数据就返回1个数据。
  */
object UdafExample {
  private def doit(sparkContext: SparkContext): Unit = {
    val sqlContext = new SQLContext(sparkContext)
    val nameRowRDD = sparkContext.parallelize(Array("xiaowang", "xiaohei","xiaowang","xiaobai"), 4).map(x=>Row(x))
    val schema = StructType(Array(StructField("name", StringType, true)))
    val nameDF = sqlContext.createDataFrame(nameRowRDD, schema)
    nameDF.registerTempTable("t_name")
    sqlContext.udf.register("strGroupCount", new StringGroupCount)
    sqlContext.sql("select name, strGroupCount(name) from t_name group by name").collect().foreach(println)
  }

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName(UdafExample.getClass.getSimpleName)
    val sparkContext = new SparkContext(conf)
    UdafExample.doit(sparkContext)
    sparkContext.stop()
  }
}
