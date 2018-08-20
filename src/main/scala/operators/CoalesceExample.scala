package operators

import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer

/**
  * Transformation
  * coalesce
  * 语法（scala）：
  * JavaRDD<T> coalesce(int numPartitions)
  *
  * JavaRDD<T> coalesce(int numPartitions,
  * boolean shuffle)
  *
  * JavaPairRDD<K,V> coalesce(int numPartitions)
  *
  * JavaPairRDD<K,V> coalesce(int numPartitions,
  * boolean shuffle)
  * 说明：
  * 用于将RDD进行重分区，使用HashPartitioner。且该RDD的分区个数等于numPartitions个数。如果shuffle设置为true，则会进行shuffle。
  */
object CoalesceExample {
  private def doit(sparkContext: SparkContext): Unit = {
    val datas = ArrayBuffer("hi", "hello", "how", "are", "you")
    val datasRDD = sparkContext.parallelize(datas, 4)
    println("RDD的分区数: " + datasRDD.partitions.length)
    val datasRDD2 = datasRDD.coalesce(2)
    println("RDD的分区数: " + datasRDD2.partitions.length)

  }

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName(CoalesceExample.getClass.getSimpleName)
    val sparkContext = new SparkContext(conf)
    CoalesceExample.doit(sparkContext)
    sparkContext.stop()
  }
}
