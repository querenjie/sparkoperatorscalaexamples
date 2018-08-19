package operators

import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer

/**
  * Transformation
  * sample
  * 语法（scala）：
  * def sample(
  * withReplacement: Boolean,
  * fraction: Double,
  * seed: Long = Utils.random.nextLong): RDD[T]
  *
  * 说明：
  * 对RDD进行抽样，其中参数withReplacement为true时表示抽样之后还放回，可以被多次抽样，false表示不放回；fraction表示抽样比例；seed为随机数种子，比如当前时间戳
  */
object SampleExample {
  private def doit(): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName(SampleExample.getClass.getSimpleName)
    val sc = new SparkContext(conf)

    val datas = ArrayBuffer(1, 2, 3, 7, 4, 5, 8)
    val datasRDD = sc.parallelize(datas)

    datasRDD.sample(false, 0.5, System.currentTimeMillis()).foreach(println(_))

    sc.stop()
  }

  def main(args: Array[String]): Unit = {
    SampleExample.doit()
  }
}
