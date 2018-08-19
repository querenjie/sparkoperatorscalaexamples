package operators

import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer

/**
  * Transformation
  * mapPartitionsWithIndex
  * 语法（scala）：
  * def mapPartitionsWithIndex[U: ClassTag](
  * f: (Int, Iterator[T]) => Iterator[U],
  * preservesPartitioning: Boolean = false): RDD[U]
  *
  * 说明：
  * 与mapPartitions类似，但输入会多提供一个整数表示分区的编号，所以func的类型是(Int, Iterator<T>) => Iterator<R>，多了一个Int
  */
object MapPartitionsWithIndex {
  private def doit(): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName(MapPartitionsExample.getClass.getSimpleName)
    val sc = new SparkContext(conf)
    val datas = List("张三1", "李四1", "王五1", "张三2", "李四2",
      "王五2", "张三3", "李四3", "王五3", "张三4")
    if (datas == None || datas == Nil || datas.isEmpty) {
      println("没数据。。。")
      None
    }
    val datasRDD = sc.parallelize(datas, 3)
    datasRDD.mapPartitionsWithIndex((i, iter) => {
      var result = new ArrayBuffer[String]()
      while (iter.hasNext) {
        result.append("分区索引:" + i + "\t" + iter.next)
      }
      result.iterator
    }, true).foreach(println)

    sc.stop()
  }

  def main(args: Array[String]): Unit = {
    MapPartitionsWithIndex.doit()
  }
}
