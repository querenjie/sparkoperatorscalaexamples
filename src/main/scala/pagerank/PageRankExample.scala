package pagerank

import org.apache.spark.{SparkConf, SparkContext}

/**
  * PageRank的例子，计算每个元素的PageRank值
  */
object PageRankExample {
  private def doit(sparkContext: SparkContext): Unit = {
    var iter = 10
    sparkContext.setCheckpointDir(".")
    val lines = sparkContext.textFile("page.txt")

    //根据边关系生成邻接表：(1,(2,3,4)) (2,(1,4)) (3,(2,5,4)) (5,(1,2,4))
    val links = lines.map(s => {
      val parts = s.split("\\s+")
      (parts(0), parts(1))
    }).distinct().groupByKey().cache()

    //对每个值都被置为1.0。  (1,1.0) (2,1.0) (3,1.0) (5,1.0)
    var ranks = links.mapValues(v => 1.0)


    /**
      * links.join(ranks).values.collect().foreach(println)的结果：
      * ((4, 1, 2),1.0) ((1, 4),1.0) ((4, 5, 2),1.0) ((4, 3, 2),1.0)
      */

    for (i <- 1 to iter) {
      val contribs = links.join(ranks).values.flatMap({
        case (urls, rank) => urls.map(url => (url, rank / urls.size))
      })
      ranks = contribs.reduceByKey(_ + _).mapValues(0.15 + 0.85 * _)
      ranks.checkpoint()
    }
    ranks.map(x=>(x._2, x._1)).sortByKey(false).map(x=>(x._2, x._1)).foreach(println)
    /*
    (4,0.5640892465003026)
    (2,0.3954759488880675)
    (1,0.3820334389970392)
    (3,0.25859239003513257)
    (5,0.22346341746813086)
     */
  }

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName(PageRankExample.getClass.getSimpleName)
    val sparkContext = new SparkContext(conf)
    PageRankExample.doit(sparkContext)
    sparkContext.stop()
  }
}
