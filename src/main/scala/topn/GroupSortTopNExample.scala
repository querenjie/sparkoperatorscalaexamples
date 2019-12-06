package topn

import org.apache.spark.{SparkConf, SparkContext}

/**
  * 读取文件，分组排序，提取每个分组的前N条数据
  */
object GroupSortTopNExample {
  private def doit(sparkContext: SparkContext, topN: Int) {
    /*
        group_sort_topn.txt中的内容：
        hadoop 23
        spark 45
        java 90
        spark 57
        spark 90
        hadoop 99
        hadoop 76
        spark 45
        spark 88
        spark 89
        hadoop 45
        hadoop 90
        java 78
        java 70
     */
    val textRDD = sparkContext.textFile("group_sort_topn.txt")
    textRDD.map(line=>(line.split(" ")(1), line.split(" ")(0))).sortByKey(false)
      .map(tuple=>(tuple._2,tuple._1)).groupByKey().map(tuple=>{
      val topNScore = tuple._2.toList.take(topN)
      (tuple._1, topNScore)
    }).foreach(tuple=>{
      val key = tuple._1
      val scoreList = tuple._2
      for (score <- scoreList) {
        println(key + " " + score)
      }
    })
  }

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName(GroupSortTopNExample.getClass.getSimpleName)
    val sparkContext = new SparkContext(conf)
    GroupSortTopNExample.doit(sparkContext, 5)
    sparkContext.stop()
  }
}
