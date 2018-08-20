package operators

import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer

/**
  * Transformation
  * distinct
  * 语法（scala）：
  * def distinct(numPartitions: Int)(implicit ord: Ordering[T] = null): RDD[T]
  * def distinct(): RDD[T]
  * 说明：
  * 对原RDD进行去重操作，返回RDD中没有重复的成员
  */
object DistinctExample {
  private def doit(): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName(DistinctExample.getClass.getSimpleName)
    val sc = new SparkContext(conf)

    val datas = ArrayBuffer("张三", "李四", "tom", "张三")

    sc.parallelize(datas).distinct().foreach(println)

    sc.stop()
  }

  def main(args: Array[String]): Unit = {
    DistinctExample.doit()
  }
}
