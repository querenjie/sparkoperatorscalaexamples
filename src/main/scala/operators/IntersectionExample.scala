package operators

import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer

/**
  * Transformation
  * intersection
  * 语法（scala）：
  * def intersection(other: RDD[T]): RDD[T]
  * 说明：
  * 返回两个RDD的交集
  */
object IntersectionExample {
  private def doit(): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName(IntersectionExample.getClass.getSimpleName)
    val sc = new SparkContext(conf)

    val datas1 = ArrayBuffer("张三", "李四", "tom")
    val datas2 = ArrayBuffer("tom", "gim")

    sc.parallelize(datas1).intersection(sc.parallelize(datas2)).foreach(println)

    sc.stop()
  }

  def main(args: Array[String]): Unit = {
    IntersectionExample.doit()
  }
}
