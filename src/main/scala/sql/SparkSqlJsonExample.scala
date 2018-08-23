package sql

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext

import scala.collection.mutable.ArrayBuffer

/**
  * SparkSQL访问json格式的文件
  */
object SparkSqlJsonExample {
  private def doit(sparkContext: SparkContext): Unit = {
    val sqlContext = new SQLContext(sparkContext)
    val studentScoreDataset = sqlContext.read.json("student_score.json")
    studentScoreDataset.registerTempTable("student_score")
    val studentAgeList = ArrayBuffer(
      "{\"name\":\"stud_01\",\"age\":18}",
      "{\"name\":\"stud_02\",\"age\":17}",
      "{\"name\":\"stud_03\",\"age\":19}",
      "{\"name\":\"stud_04\",\"age\":20}",
      "{\"name\":\"stud_05\",\"age\":21}")
    val studentAgeDataset = sqlContext.read.json(sparkContext.parallelize(studentAgeList))
    studentAgeDataset.registerTempTable("student_info")

    val sql = "select a.name, a.score, b.age from student_score a, student_info b where a.name = b.name and a.score >= 80"
    val resultDataset = sqlContext.sql(sql)
    resultDataset.rdd.foreach(row => println("name:" + row(0) + "\t" + "score:" + row(1) + "\t" + "age:" + row(2)))
  }

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName(SparkSqlJsonExample.getClass.getSimpleName)
    val sparkContext = new SparkContext(conf)
    SparkSqlJsonExample.doit(sparkContext)
    sparkContext.stop()
  }
}
