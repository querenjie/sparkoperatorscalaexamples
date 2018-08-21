package order

import org.apache.spark.{SparkConf, SparkContext}

/**
  * 二次排序的例子
  * KeyWrapper是两个Key都按照从小到大的顺序排序
  * KeyWrapper2是两个Key都按照从大到小的顺序排序
  * KeyWrapper3是firstKey按照从大到小排序，sencondKey是按照从小到大排序
  * KeyWrapper4是firstKey按照从小到大排序，sencondKey是按照从大到小排序
  */
object SecondarySortExample {
  private def doit(sparkContext: SparkContext): Unit = {
    val textRDD = sparkContext.textFile("sort.txt")
    textRDD.map(line => {
      val arr = line.split(",")
      val firstKey = arr(0).trim.toInt
      val secondKey = arr(2).trim.toInt
      (new KeyWrapper4(firstKey, secondKey), line)
    }).sortByKey().map(tuple => tuple._2).foreach(println)
  }

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName(SecondarySortExample.getClass.getSimpleName)
    val sparkContext = new SparkContext(conf)
    SecondarySortExample.doit(sparkContext)
    sparkContext.stop()
  }
}
