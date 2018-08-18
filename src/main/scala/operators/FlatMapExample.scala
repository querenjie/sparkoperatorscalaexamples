package operators

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Transformation
  * flatMap
  * 语法（scala）：
  * def flatMap[U: ClassTag](f: T => TraversableOnce[U]): RDD[U]
  * 说明：
  * 与map类似，但每个输入的RDD成员可以产生0或多个输出成员
  */
object FlatMapExample {
  private def doit(): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName(FlatMapExample.getClass.getSimpleName)
    val sc = new SparkContext(conf)

    val data = Array(
      "aa,bb,cc",
      "cxf,spring,struts2",
      "java,C++,javaScript"
    )
    val dataRDD = sc.parallelize(data)
    val flatMappedRDD = dataRDD.flatMap(v => v.split(","))
    flatMappedRDD.foreach(println)

    sc.stop()
  }

  def main(args: Array[String]): Unit = {
    FlatMapExample.doit()
  }
}
