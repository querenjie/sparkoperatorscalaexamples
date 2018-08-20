package operators

import org.apache.spark.{Partitioner, SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer
import scala.util.Random

/**
  * Transformation
  * repartitionAndSortWithinPartitions
  * 语法（scala）：
  * JavaPairRDD<K,V> repartitionAndSortWithinPartitions(Partitioner partitioner)
  *
  * JavaPairRDD<K,V> repartitionAndSortWithinPartitions(Partitioner partitioner,
  *                                                   java.util.Comparator<K> comp)
  * 说明：
  * 根据给定的Partitioner重新分区，并且每个分区内根据comp实现排序。
  */
object RepartitionAndSortWithinPartitionsExample {
  private def doit(sparkContext: SparkContext): Unit = {
    val datas = new Array[String](1000)
    val random = new Random(1)
    for (i <- 0 until 10; j <- 0 until 100) {
      val index : Int = i * 100 + j
      datas(index) = "product" + random.nextInt(10) + ",url" + random.nextInt(100)
    }
    val datasRDD = sparkContext.parallelize(datas)
    val pairRDD = datasRDD.map(line => (line, 1)).reduceByKey((a, b) => a + b)
    pairRDD.repartitionAndSortWithinPartitions(new Partitioner {
      override def numPartitions: Int = 10

      override def getPartition(key: Any): Int = {
        val str = String.valueOf(key)
        str.substring(7, str.indexOf(",")).toInt
      }
    }).foreach(println)
  }

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName(RepartitionAndSortWithinPartitionsExample.getClass.getSimpleName)
    val sparkContext = new SparkContext(conf)
    RepartitionAndSortWithinPartitionsExample.doit(sparkContext)
    sparkContext.stop()
  }
}
