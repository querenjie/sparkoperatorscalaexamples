package operators

import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer

/**
  * Transformation
  * groupByKey
  * 语法（scala）：
  * def groupBy[K](f: T => K)(implicit kt: ClassTag[K]): RDD[(K, Iterable[T])]
  * def groupBy[K](      f: T => K,      numPartitions: Int)(implicit kt: ClassTag[K]): RDD[(K, Iterable[T])]
  * def groupBy[K](f: T => K, p: Partitioner)(implicit kt: ClassTag[K], ord: Ordering[K] = null)      : RDD[(K, Iterable[T])]
  * # PairRDDFunctions类中
  * def groupByKey(partitioner: Partitioner): RDD[(K, Iterable[V])]
  * def groupByKey(numPartitions: Int): RDD[(K, Iterable[V])]
  * def groupByKey(): RDD[(K, Iterable[V])]
  * 说明：
  * 对<key, value>结构的RDD进行类似RMDB的group by聚合操作，具有相同key的RDD成员的value会被聚合在一起，返回的RDD的结构是(key, Iterator<value>)
  */
object GroupByKeyExample {
  private def doit(): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName(GroupByKeyExample.getClass.getSimpleName)
    val sc = new SparkContext(conf)

    val datas = ArrayBuffer(1, 2, 3, 4, 5, 6, 7, 8, 9)

    sc.parallelize(datas).groupBy(v => {
      if (v % 2 == 0) {
        "偶数"
      } else {
        "奇数"
      }
    }).foreach(println)

    sc.stop()
  }

  private def doit2(): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName(GroupByKeyExample.getClass.getSimpleName)
    val sc = new SparkContext(conf)

    val datas = ArrayBuffer("dog", "tiger", "lion", "cat", "spider", "eagle")

    sc.parallelize(datas).keyBy(v => v.length).groupByKey().foreach(println)

    sc.stop()
  }

  def main(args: Array[String]): Unit = {
    GroupByKeyExample.doit()
    println("------------------------------------------------------")
    println("以下是调用doit2()的结果：")
    GroupByKeyExample.doit2()
  }
}
