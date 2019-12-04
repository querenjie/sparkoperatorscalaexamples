package operators

import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer

/**
  * Transformation
  * aggregateByKey
  * 举例：
  * 通过scala集合以并行化方式创建一个RDD
  * scala> val pairRdd = sc.parallelize(List(("cat",2),("cat",5),("mouse",4),("cat",12),("dog",12),("mouse",2)),2)
  * pairRdd 这个RDD有两个区，一个区中存放的是：
  * ("cat",2),("cat",5),("mouse",4)
  * 另一个分区中存放的是：
  * ("cat",12),("dog",12),("mouse",2)
  * 然后，执行下面的语句
  * scala > pairRdd.aggregateByKey(100)(math.max(_ , _),  _ + _ ).collect
  * 结果：
  * res0: Array[(String,Int)] = Array((dog,100),(cat,200),(mouse,200))
  * 下面是以上语句执行的原理详解：
  * aggregateByKey的意思是：按照key进行聚合
  * 第一步：将每个分区内key相同数据放到一起
  * 分区一
  * ("cat",(2,5)),("mouse",4)
  * 分区二
  * ("cat",12),("dog",12),("mouse",2)
  * 第二步：局部求最大值
  * 对每个分区应用传入的第一个函数，math.max(_ , _)，这个函数的功能是求每个分区中每个key的最大值
  * 这个时候要特别注意，aggregateByKe(100)(math.max(_ , _),_+_)里面的那个100，其实是个初始值
  * 在分区一中求最大值的时候,100会被加到每个key的值中，这个时候每个分区就会变成下面的样子
  * 分区一
  * ("cat",(2,5，100)),("mouse",(4，100))
  * 然后求最大值后变成：
  * ("cat",100), ("mouse",100)
  * 分区二
  * ("cat",(12,100)),("dog",(12.100)),("mouse",(2,100))
  * 求最大值后变成：
  * ("cat",100),("dog",100),("mouse",100)
  * 第三步：整体聚合
  * 将上一步的结果进一步的合成，这个时候100不会再参与进来
  * 最后结果就是：
  * (dog,100),(cat,200),(mouse,200)
  */
object AggregateByKeyExample {
  private def doit(sparkContext: SparkContext): Unit = {
    val datas = ArrayBuffer((1, 3), (1, 2), (1, 4), (2, 5))
    val datasRDD = sparkContext.parallelize(datas, 2)
    //datasRDD.aggregateByKey(0)(seq, comb).foreach(println)
    datasRDD.aggregateByKey(0)(seq, comb).collect()
  }

  private def seq(a: Int, b: Int): Int = {
    println("seq: " + a + "\t" + b)
    return math.max(a, b)
  }

  private def comb(a: Int, b: Int): Int = {
    println("comb: " + a + "\t" + b)
    return a + b
  }

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName(AggregateByKeyExample.getClass.getSimpleName)
    val sparkContext = new SparkContext(conf)
    AggregateByKeyExample.doit(sparkContext)
    sparkContext.stop()
  }
}
