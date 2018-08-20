package operators

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Transformation
  * cogroup
  * 语法（scala）：
  * JavaPairRDD<K,scala.Tuple2<Iterable<V>,Iterable<W>>> cogroup(JavaPairRDD<K,W> other,
  * Partitioner partitioner)
  *
  * JavaPairRDD<K,scala.Tuple3<Iterable<V>,Iterable<W1>,Iterable<W2>>> cogroup(JavaPairRDD<K,W1> other1,
  * JavaPairRDD<K,W2> other2,
  * Partitioner partitioner)
  *
  * JavaPairRDD<K,scala.Tuple4<Iterable<V>,Iterable<W1>,Iterable<W2>,Iterable<W3>>> cogroup(JavaPairRDD<K,W1> other1,
  * JavaPairRDD<K,W2> other2,
  * JavaPairRDD<K,W3> other3,
  * Partitioner partitioner)
  *
  * JavaPairRDD<K,scala.Tuple2<Iterable<V>,Iterable<W>>> cogroup(JavaPairRDD<K,W> other)
  *
  * JavaPairRDD<K,scala.Tuple3<Iterable<V>,Iterable<W1>,Iterable<W2>>> cogroup(JavaPairRDD<K,W1> other1,
  * JavaPairRDD<K,W2> other2)
  *
  * JavaPairRDD<K,scala.Tuple4<Iterable<V>,Iterable<W1>,Iterable<W2>,Iterable<W3>>> cogroup(JavaPairRDD<K,W1> other1,
  * JavaPairRDD<K,W2> other2,
  * JavaPairRDD<K,W3> other3)
  *
  * JavaPairRDD<K,scala.Tuple2<Iterable<V>,Iterable<W>>> cogroup(JavaPairRDD<K,W> other,
  * int numPartitions)
  *
  * JavaPairRDD<K,scala.Tuple3<Iterable<V>,Iterable<W1>,Iterable<W2>>> cogroup(JavaPairRDD<K,W1> other1,
  * JavaPairRDD<K,W2> other2,
  * int numPartitions)
  *
  * JavaPairRDD<K,scala.Tuple4<Iterable<V>,Iterable<W1>,Iterable<W2>,Iterable<W3>>> cogroup(JavaPairRDD<K,W1> other1,
  * JavaPairRDD<K,W2> other2,
  * JavaPairRDD<K,W3> other3,
  * int numPartitions)
  * 说明：
  * cogroup:对多个RDD中的KV元素，每个RDD中相同key中的元素分别聚合成一个集合。与reduceByKey不同的是针对两个RDD中相同的key的元素进行合并。
  */
object CogroupExample {
  private def doit(sparkContext: SparkContext): Unit = {
    val datas1 = List((1, "苹果"), (2, "梨"), (3, "香蕉"), (4, "石榴"))
    val datas2 = List((1, 7), (2, 3), (3, 8), (4, 3))
    val datas3 = List((1, "7"), (2, "3"), (3, "8"), (4, "3"), (4, "4"), (4, "5"), (4, "6"))

    sparkContext.parallelize(datas1).cogroup(sparkContext.parallelize(datas2), sparkContext.parallelize(datas3)).foreach(println)
  }

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName(CogroupExample.getClass.getSimpleName)
    val sparkContext = new SparkContext(conf)
    CogroupExample.doit(sparkContext)

    sparkContext.stop()
  }
}
