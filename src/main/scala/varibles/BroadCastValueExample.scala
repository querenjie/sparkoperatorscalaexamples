package varibles

import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer

/**
  * 广播变量的使用
  */
object BroadCastValueExample {
  private def doit(sparkContext: SparkContext): Unit = {
    val f = 3
    val broadcastF = sparkContext.broadcast(f)
    val datas = ArrayBuffer(1,2,3,4,5)
    sparkContext.parallelize(datas).map(x => x * broadcastF.value).foreach(println)
  }

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName(BroadCastValueExample.getClass.getSimpleName)
    val sparkContext = new SparkContext(conf)
    BroadCastValueExample.doit(sparkContext)
    sparkContext.stop()
  }
}
