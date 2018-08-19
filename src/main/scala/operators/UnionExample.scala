package operators

import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer

/**
  * Transformation
  * union
  * 语法（scala）：
  * def union(other: RDD[T]): RDD[T]
  *
  * 说明：
  * 合并两个RDD，不去重，要求两个RDD中的元素类型一致
  */
object UnionExample {
  private def doit(): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName(UnionExample.getClass.getSimpleName)
    val sc = new SparkContext(conf)

    val datas1 = ArrayBuffer("张三", "李四")
    val datas2 = ArrayBuffer("tom", "gim")

    val datas1RDD = sc.parallelize(datas1)
    val datas2RDD = sc.parallelize(datas2)

    datas1RDD.union(datas2RDD).foreach(println(_))

    sc.stop()
  }

  def main(args: Array[String]): Unit = {
    UnionExample.doit()
  }
}
