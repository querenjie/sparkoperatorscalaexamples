package operators.action

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Transformation
  * takeOrdered
  * 语法（scala）：
  * java.util.List<T> takeOrdered(int num)
  *
  * java.util.List<T> takeOrdered(int num,
  *                           java.util.Comparator<T> comp)
  * 说明：
  * 用于从RDD中，按照默认（升序）或指定排序规则，返回前num个元素。
  */
object TakeOrderedExample {
  private def doit(sparkContext: SparkContext): Unit = {
    sparkContext.parallelize(Array(5,6,2,1,7,8)).takeOrdered(3)(new Ordering[Int]() {
      override def compare(x: Int, y: Int): Int = y.compareTo(x)
    }).foreach(println)

  }

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName(TakeOrderedExample.getClass.getSimpleName)
    val sparkContext = new SparkContext(conf)
    TakeOrderedExample.doit(sparkContext)
    sparkContext.stop()
  }
}
