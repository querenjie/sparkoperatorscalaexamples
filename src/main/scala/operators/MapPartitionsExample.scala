package operators

import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer

/**
  * Transformation
  * mapPartitions
  * 语法（scala）：
  * def mapPartitions[U: ClassTag](
  * f: Iterator[T] => Iterator[U],
  * preservesPartitioning: Boolean = false): RDD[U]
  *
  * 说明：
  * 与Map类似，但map中的func作用的是RDD中的每个元素，而mapPartitions中的func作用的对象是RDD的一整个分区。
  * 所以func的类型是Iterator<T> => Iterator<U>，其中T是输入RDD元素的类型。
  * preservesPartitioning表示是否保留输入函数的partitioner，默认false。
  */
object MapPartitionsExample {
  private def doit() {
    val conf = new SparkConf().setMaster("local").setAppName(MapPartitionsExample.getClass.getSimpleName)
    val sc = new SparkContext(conf)

    val datas : List[String] = List("张三1", "李四1", "王五1", "张三2", "李四2",
      "王五2", "张三3", "李四3", "王五3", "张三4")
    if (datas == None || datas == Nil || datas.isEmpty) {
      println("没数据。。。")
      None
    }
    sc.parallelize(datas, 3).mapPartitions(n => {
      var count = 0
      var result = ArrayBuffer[String]()
      while (n.hasNext) {
        result.append("分区索引:" + count + "\t" + n.next)
        count = count + 1
      }
      result.iterator
    }).foreach(println)

    sc.stop()
  }

  def main(args: Array[String]): Unit = {
    MapPartitionsExample.doit()
  }
}
