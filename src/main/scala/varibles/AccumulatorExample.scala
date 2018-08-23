package varibles

import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer

/**
  * Accumulator变量的应用。在Driver中获取累计值
  */
object AccumulatorExample {
  private def doit(sparkContext: SparkContext): Unit = {
    val datas = ArrayBuffer(1,2,3,4,5)
    val sum = sparkContext.accumulator(0, "Accumulator Example")
    sparkContext.parallelize(datas).foreach(x => sum += x)
    println("sum = " + sum.value)
  }

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName(AccumulatorExample.getClass.getSimpleName)
    val sparkContext = new SparkContext(conf)
    AccumulatorExample.doit(sparkContext)
    sparkContext.stop()
  }
}
