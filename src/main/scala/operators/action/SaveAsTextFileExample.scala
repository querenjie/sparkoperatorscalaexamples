package operators.action

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Action
  * saveAsTextFile
  * 语法（java）：
  * void saveAsTextFile(String path)
  *
  * void saveAsTextFile(String path,
  * Class<? extends org.apache.hadoop.io.compress.CompressionCodec> codec)
  * 说明：
  * 将RDD转换为文本内容并保存至路径path下，可能有多个文件(和partition数有关)。路径path可以是本地路径或HDFS地址，转换方法是对RDD成员调用toString函数
  */
object SaveAsTextFileExample {
  private def doit(sparkContext: SparkContext): Unit = {
    sparkContext.parallelize(Array(5, 6, 2, 1, 7, 8),3).saveAsTextFile("d://test/spark_data/aaa")
  }

  def main(args: Array[String]): Unit = {
    SaveAsTextFileExample.doit(new SparkContext(new SparkConf().setMaster("local").setAppName(SaveAsTextFileExample.getClass.getSimpleName)))
  }
}
