package operators.action

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Action
  * countByKey
  * 语法（java）：
  * java.util.Map<K,Long> countByKey()
  * 说明：
  * 仅适用于(K, V)类型，对key计数，返回(K, Int)
  */
object CountByKeyExample {
  private def doit(sparkContext: SparkContext): Unit = {
    sparkContext.parallelize(Array(("A", 1), ("B", 6), ("A", 2), ("C", 1), ("A", 7), ("A", 8))).countByKey().foreach(println)
  }

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName(CountByKeyExample.getClass.getSimpleName)
    val sparkContext = new SparkContext(conf)
    CountByKeyExample.doit(sparkContext)
    sparkContext.stop()
  }
}
