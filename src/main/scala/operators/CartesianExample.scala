package operators

import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer

/**
  * Transformation
  * cartesian
  * 语法（scala）：
  * static <U> JavaPairRDD<T,U> cartesian(JavaRDDLike<U,?> other)
  * 说明：
  * 两个RDD进行笛卡尔积合并
  */
object CartesianExample {
  private def doit(sparkContext: SparkContext): Unit = {
    val names = ArrayBuffer("张三", "李四", "王五")
    val scores = ArrayBuffer(60, 70, 80)

    val namesRDD = sparkContext.parallelize(names)
    val scoresRDD = sparkContext.parallelize(scores)

    namesRDD.cartesian(scoresRDD).foreach(println)
  }

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName(CartesianExample.getClass.getSimpleName)
    val sparkContext = new SparkContext(conf)
    CartesianExample.doit(sparkContext)

    sparkContext.stop()
  }
}
