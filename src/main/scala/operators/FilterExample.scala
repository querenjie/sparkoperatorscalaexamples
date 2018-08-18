package operators

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Transformation
  * filter
  * 语法（scala）：
  * def filter(f: T => Boolean): RDD[T]
  * 说明：
  * 对元素进行过滤，对每个元素应用f函数，返回值为true的元素在RDD中保留，返回为false的将过滤掉
  */
object FilterExample {
  private def doit(): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName(FilterExample.getClass.getSimpleName)
    val sc = new SparkContext(conf)

    val datas = Array(1, 2, 3, 7, 4, 5, 8)
    val datasRDD = sc.parallelize(datas)
    val filteredRDD = datasRDD.filter(x => {x >= 3})
    filteredRDD.foreach(println(_))
  }

  def main(args: Array[String]): Unit = {
    FilterExample.doit()
  }
}
