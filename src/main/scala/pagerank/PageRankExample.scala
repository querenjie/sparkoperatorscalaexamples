package pagerank

import org.apache.spark.{SparkConf, SparkContext}

/**
  * PageRank的例子，计算每个元素的PageRank值
  * pagerank值反映出url被引用的频繁程度
  */
object PageRankExample {
  private def doit(sparkContext: SparkContext): Unit = {
    var iter = 10
    sparkContext.setCheckpointDir(".")
    /**
      * page.txt中的第一列为url的id，第二列为在第一列的url中引用到的url的id
      * page.txt中的内容：
      * 1 2
      * 1 2
      * 1 3
      * 1 4
      * 2 1
      * 2 4
      * 3 2
      * 3 5
      * 5 1
      * 5 2
      * 5 4
      * 3 4
      */
    val lines = sparkContext.textFile("page.txt")

    //根据边关系生成邻接表：(1,(2,3,4)) (2,(1,4)) (3,(2,5,4)) (5,(1,2,4))
    val links = lines.map(s => {
      val parts = s.split("\\s+")
      (parts(0), parts(1))
    }).distinct().groupByKey().cache()

    //对每个值都被置为1.0。  (1,1.0) (2,1.0) (3,1.0) (5,1.0)
    var ranks = links.mapValues(v => 1.0)
    //---------to remove them after test--------------------
    println("ranks：")
    ranks.foreach(println)
    //---------------------------------------------------

    /*
      搞多次迭代计算每个url的rank值。
      每次迭代用新的rank值替代旧的rank值然后重新计算出新的rank值以便替换掉下一轮迭代中的每个url的rank值。
      rank值的计算过程：
        初始时对每个被引用的url设定rank=1
        在每次循环中做这些事情：
          取出每个url中被引用的url以及数量n，计算每个被引用url的贡献值=rank/n,得出n个元组(被引用的url, 贡献值)，
          循环中得出的总共的元组数量为多个url中总共引用的url的数量，其中可能会出现重复的url
          根据被引用的url分组求和贡献值，得到多个元组(被引用的url, 总贡献值)，其中的key就不会有重复了。
          然后计算出每个被引用的url的新的rank值=0.15+0.85*原来的rank值，然后在下一次循环开始就重新设置每个url的rank值为新的rank值。
     */
    for (i <- 1 to iter) {
      //---------to remove them after test--------------------
      var links_join_ranks = links.join(ranks)
      println("第" + i + "次links.join(ranks)的结果：")
      links_join_ranks.foreach(println)
      //---------------------------------------------------
      /**
        * 第1次links.join(ranks)的结果：
        * (5,(CompactBuffer(4, 1, 2),1.0))
        * (2,(CompactBuffer(1, 4),1.0))
        * (3,(CompactBuffer(4, 5, 2),1.0))
        * (1,(CompactBuffer(4, 3, 2),1.0))
        * 第2次links.join(ranks)的结果：
        * (5,(CompactBuffer(4, 1, 2),0.43333333333333335))
        * (2,(CompactBuffer(1, 4),1.0))
        * (3,(CompactBuffer(4, 5, 2),0.43333333333333335))
        * (1,(CompactBuffer(4, 3, 2),0.8583333333333333))
        * 第3次links.join(ranks)的结果：
        * (5,(CompactBuffer(4, 1, 2),0.2727777777777778))
        * (2,(CompactBuffer(1, 4),0.6387499999999999))
        * (3,(CompactBuffer(4, 5, 2),0.3931944444444444))
        * (1,(CompactBuffer(4, 3, 2),0.6977777777777778))
        * ......
        */
      val contribs = links.join(ranks).values.flatMap({
        case (urls, rank) => urls.map(url => {(url, rank / urls.size)})
      })
      //---------to remove them after test--------------------
      println("第" + i + "次contribs的结果：")
      contribs.foreach(println)
      //---------------------------------------------------

      /**
        * 第1次contribs的结果：
        * (4,0.3333333333333333)
        * (1,0.3333333333333333)
        * (2,0.3333333333333333)
        * (1,0.5)
        * (4,0.5)
        * (4,0.3333333333333333)
        * (5,0.3333333333333333)
        * (2,0.3333333333333333)
        * (4,0.3333333333333333)
        * (3,0.3333333333333333)
        * (2,0.3333333333333333)
        * 第2次contribs的结果：
        * (4,0.14444444444444446)
        * (1,0.14444444444444446)
        * (2,0.14444444444444446)
        * (1,0.5)
        * (4,0.5)
        * (4,0.14444444444444446)
        * (5,0.14444444444444446)
        * (2,0.14444444444444446)
        * (4,0.2861111111111111)
        * (3,0.2861111111111111)
        * (2,0.2861111111111111)
        * 第3次contribs的结果：
        * (4,0.09092592592592592)
        * (1,0.09092592592592592)
        * (2,0.09092592592592592)
        * (1,0.31937499999999996)
        * (4,0.31937499999999996)
        * (4,0.1310648148148148)
        * (5,0.1310648148148148)
        * (2,0.1310648148148148)
        * (4,0.2325925925925926)
        * (3,0.2325925925925926)
        * (2,0.2325925925925926)
        * ......
        */

      ranks = contribs.reduceByKey(_ + _).mapValues(0.15 + 0.85 * _)
      //---------to remove them after test--------------------
      println("第" + i + "次ranks的结果：")
      ranks.foreach(println)
      //---------------------------------------------------
      /**
        * 第1次ranks的结果：
        * (4,1.4249999999999996)
        * (5,0.43333333333333335)
        * (2,1.0)
        * (3,0.43333333333333335)
        * (1,0.8583333333333333)
        * 第2次ranks的结果：
        * (4,1.0637500000000002)
        * (5,0.2727777777777778)
        * (2,0.6387499999999999)
        * (3,0.3931944444444444)
        * (1,0.6977777777777778)
        * 第3次ranks的结果：
        * (4,0.8078645833333332)
        * (5,0.2614050925925926)
        * (2,0.5363958333333333)
        * (3,0.34770370370370374)
        * (1,0.49875578703703694)
        * ......
        */

      ranks.checkpoint()
    }

    //最后根据rank值从大到小列出url和对应的rank值
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
