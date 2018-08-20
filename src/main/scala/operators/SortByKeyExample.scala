package operators

import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer

/**
  * Transformation
  * sortByKey
  * 语法（scala）：
  * JavaRDD<T> sortBy(Function<T,S> f,
  * boolean ascending,
  * int numPartitions)
  * JavaPairRDD<K,V> sortByKey()
  * JavaPairRDD<K,V> sortByKey(boolean ascending)
  * JavaPairRDD<K,V> sortByKey(boolean ascending,
  * int numPartitions)
  * JavaPairRDD<K,V> sortByKey(java.util.Comparator<K> comp)
  * JavaPairRDD<K,V> sortByKey(java.util.Comparator<K> comp,
  * boolean ascending)
  * JavaPairRDD<K,V> sortByKey(java.util.Comparator<K> comp,
  * boolean ascending,
  * int numPartitions)
  * 说明：
  * 对<key, value>结构的RDD进行升序或降序排列
  * 参数：
  * comp：排序时的比较运算方式。
  * ascending：false降序；true升序。
  */
object SortByKeyExample {
  private def doit(sparkContext: SparkContext): Unit = {
    val datas = ArrayBuffer(60, 70, 80, 55, 45, 75)
    val dataRDD = sparkContext.parallelize(datas)
    dataRDD.sortBy(v => v, false, 1).foreach(println)
  }

  private def doit2(sparkContext: SparkContext): Unit = {
    sparkContext.parallelize(List((3, 3), (2, 2), (1, 4), (2, 3))).sortByKey(false).foreach(println)
  }

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName(SortByKeyExample.getClass.getSimpleName)
    val sparkContext = new SparkContext(conf)
    SortByKeyExample.doit(sparkContext)
    println("--------------------------------------------------------")
    println("以下是调用doit2()的输出：")
    SortByKeyExample.doit2(sparkContext)
    sparkContext.stop()
  }
}
