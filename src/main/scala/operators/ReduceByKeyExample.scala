package operators

import java.io.{FileNotFoundException, IOException}

import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Transformation
  * reduceByKey
  * 语法（scala）：
  * def reduceByKey(partitioner: Partitioner, func: (V, V) => V): RDD[(K, V)]
  * def reduceByKey(func: (V, V) => V, numPartitions: Int): RDD[(K, V)]
  * def reduceByKey(func: (V, V) => V): RDD[(K, V)]
  * 说明：
  * 对<key, value>结构的RDD进行聚合，对具有相同key的value调用func来进行reduce操作，func的类型必须是(V, V) => V
  */
object ReduceByKeyExample {
  private def doit(sparkContext: SparkContext): Unit = {
    try {
//      val text = sparkContext.textFile("hdfs://appcluster/test/wordcount/in/inputFile1.txt", 10000)
      val text = sparkContext.textFile("hdfs://192.168.1.20:8020/test/wordcount/in/inputFile1.txt")
      text.flatMap(line => line.split(" ")).map(word => (word, 1)).reduceByKey(_ + _).foreach(println)
    } catch {
      case ex: FileNotFoundException => {
        println("找不到文件:" + ex.toString)
      }
      case ex: IOException => {
        println("IO error:" + ex.toString)
      }
      case ex: Exception => {
        println("Exception:" + ex.toString)
      }
    }
  }

  def main(args: Array[String]): Unit = {
    //在本地运行直接访问hdfs
    val spark = SparkSession.builder().appName(ReduceByKeyExample.getClass.getSimpleName).master("local[*]").getOrCreate()
    //自动提交到集群上运行，但会有问题，so we do not recommend to use it in IDE.
//    val spark = SparkSession.builder().appName(ReduceByKeyExample.getClass.getSimpleName).master("spark://192.168.1.20:7077").getOrCreate()

    val sc = spark.sparkContext
//    val conf = new SparkConf().setMaster("spark://192.168.1.20:7077").setAppName(ReduceByKeyExample.getClass.getSimpleName)
//    val sc = new SparkContext(conf)
    ReduceByKeyExample.doit(sc)
    sc.stop()
  }
}
