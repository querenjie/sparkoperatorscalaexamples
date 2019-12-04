package operators.action

import org.apache.spark.{SparkConf, SparkContext}

object reduceExample {
  private def doit(sparkContext: SparkContext): Unit = {
    val item = sparkContext.parallelize(1 to 100, 3)
    val value = item.reduce((first, second) => first + second)
    println(value)
    val value2 = item.reduce(_ + _)
    println(value2)
    val item2 = sparkContext.parallelize(List(1,2,3))
    val value3 = item2.filter(_ > 4).reduce(_ + _)
    println("value3 = " + value3)
  }

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName(reduceExample.getClass.getSimpleName)
    val sparkContext = new SparkContext(conf)
    reduceExample.doit(sparkContext)
    sparkContext.stop()
  }
}
