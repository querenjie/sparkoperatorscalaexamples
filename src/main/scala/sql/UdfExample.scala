package sql

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SQLContext}

import scala.collection.mutable.ArrayBuffer

/**
  * UDF的例子。UDF的特点是传入多个数据就返回多个数据。
  */
object UdfExample {
  private def doit(sparkContext: SparkContext): Unit = {
    val sqlContext = new SQLContext(sparkContext)
    val nameArray = ArrayBuffer("name1", "name2", "name3", "name4")
    val nameRDD = sparkContext.parallelize(nameArray, 4)
    val nameRowRDD = nameRDD.map(x => Row(x))
    val schema = StructType(Array(StructField("name", StringType, false)))
    val nameDF = sqlContext.createDataFrame(nameRowRDD, schema)
    nameDF.registerTempTable("t_name")

    sqlContext.udf.register("length", (str: String) => str.length)
//    sqlContext.udf.register("length", myUDF)
    sqlContext.sql("select name, length(name) as length from t_name").show();
  }

//  private val myUDF = (str: String) => {
//    str.length
//  }

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName(UdfExample.getClass.getSimpleName)
    val sparkContext = new SparkContext(conf)
    UdfExample.doit(sparkContext)
    sparkContext.stop()
  }
}
