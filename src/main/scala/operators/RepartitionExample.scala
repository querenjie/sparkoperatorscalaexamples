package operators

import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer

/**
  * Transformation
  * repartition
  * 语法（scala）：
  * JavaRDD<T> repartition(int numPartitions)
  *
  * JavaPairRDD<K,V> repartition(int numPartitions)
  * 说明：
  * 该函数其实就是coalesce函数第二个参数为true的实现
  */
object RepartitionExample {
  private def doit(sparkContext: SparkContext): Unit = {
    val datas = ArrayBuffer("hi", "hello", "how", "are", "you")
    val datasRDD = sparkContext.parallelize(datas, 2)
    println("分区数量：" + datasRDD.partitions.length)
    val datasRDD2 = datasRDD.repartition(4)
    println("分区数量：" + datasRDD2.partitions.length)
  }

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName(RepartitionExample.getClass.getSimpleName)
    val sparkContext = new SparkContext(conf)
    RepartitionExample.doit(sparkContext)
    sparkContext.stop()
  }
}
