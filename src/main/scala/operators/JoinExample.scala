package operators

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Transformation
  * join
  * 语法（scala）：
  * JavaPairRDD<K,scala.Tuple2<V,W>> join(JavaPairRDD<K,W> other)
  * JavaPairRDD<K,scala.Tuple2<V,W>> join(
  * JavaPairRDD<K,W> other,
  * int numPartitions)
  * JavaPairRDD<K,scala.Tuple2<V,W>> join(
  * JavaPairRDD<K,W> other,
  * Partitioner partitioner)
  * 说明：
  * 对<K, V>和<K, W>进行join操作，返回(K, (V, W))外连接函数为leftOuterJoin、rightOuterJoin和fullOuterJoin
  */
object JoinExample {
  private def doit(sparkContext: SparkContext): Unit = {
    val products = List((1, "苹果"), (2, "梨"), (3, "香蕉"), (4, "石榴"))
    val counts = List((1, 7), (2, 3), (3, 8), (4, 3), (5, 9))

    sparkContext.parallelize(products).join(sparkContext.parallelize(counts)).sortByKey(true).foreach(println)
  }

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName(JoinExample.getClass.getSimpleName)
    val sparkContext = new SparkContext(conf)
    JoinExample.doit(sparkContext)

    sparkContext.stop()
  }
}
