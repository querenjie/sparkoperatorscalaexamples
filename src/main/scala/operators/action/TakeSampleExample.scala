package operators.action

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Transformation
  * takeSample
  * 语法（scala）：
  * static java.util.List<T> takeSample(boolean withReplacement,
  * int num,
  * long seed)
  * 说明：
  * 和sample用法相同，只不第二个参数换成了个数。返回也不是RDD，而是collect。
  */
object TakeSampleExample {
  private def doit(sparkContext: SparkContext): Unit = {
    sparkContext.parallelize(1 to 10).takeSample(false, 3, 1).foreach(println)
  }

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName(TakeSampleExample.getClass.getSimpleName)
    val sparkContext = new SparkContext(conf)
    TakeSampleExample.doit(sparkContext)
    sparkContext.stop()
  }
}
