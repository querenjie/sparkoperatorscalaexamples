package examples.relationship

import org.apache.spark.{SparkConf, SparkContext}

object RelationshipStatistics {
  private def doit(sparkContext: SparkContext): Unit = {
    //定义一组假设的朋友关系数据
    val relationData = sparkContext.parallelize(Seq("A,B", "A,C", "A,D", "A,E", "B,A", "B,C", "B,D", "C,A", "C,B", "D,A", "D,B", "E,A", "E,F", "F,E"))
    //步骤1：把A,B -> (B,A)。这一步map的最终数据变成：
    /*
      (B,A)
      (C,A)
      (D,A)
      (E,A)
      (A,B)
      (C,B)
      (D,B)
      (A,C)
      (B,C)
      (A,D)
      (B,D)
      (A,E)
      (F,E)
      (E,F)
     */
    val step1 = relationData.map(relation => {
      val splitArray = relation.split(",")
      (splitArray(1), splitArray(0))
    })

    /*
      步骤2：分组合并，表示出某人拥有哪些朋友。最后显示：
      (B,CompactBuffer(A, C, D))
      (A,CompactBuffer(B, C, D, E))
      (C,CompactBuffer(A, B))
      (E,CompactBuffer(A, F))
      (F,CompactBuffer(E))
      (D,CompactBuffer(A, B))
     */
    val step2 = step1.groupByKey();

    /*
      步骤3：为了生成人与人的全体一对一关系，将分组结果中的人两两配对。最后显示：
      ((A,C),B)((A,D),B)((C,A),B)((C,D),B)((D,A),B)((D,C),B)
      ((B,C),A)((B,D),A)((B,E),A)((C,B),A)((C,D),A)((C,E),A)((D,B),A)((D,C),A)((D,E),A)((E,B),A)((E,C),A)((E,D),A)
      ((A,B),C)((B,A),C)
      ((A,F),E)((F,A),E)
      ((A,B),D)((B,A),D)
      于是可以认为：
      (A,C)有共同好友B，(A,D)有共同好友B，(C,A)有共同好友B，(C,D)有共同好友B，(D,A)有共同好友B，(D,C)有共同好友B，
      (B,C)(B,D)(B,E)(C,B)(C,D)(C,E)(D,B)(D,C)(D,E)(E,B)(E,C)(E,D)有共同好友A，
      (A,B)(B,A)有共同好友C，
      (A,F)(F,A)有共同好友E，
      (A,B)(B,A)有共同好友D
     */
    val step3 = step2.flatMap(tuple=> {
      val relationArray = tuple._2
      for (i <- relationArray; j <- relationArray; if (i != j)) yield (i, j)->tuple._1
    })

    /*
      步骤4：再基于步骤3的结果上根据key合并结果，得到每一对人有哪些共同朋友。结果如下：
      ((B,A),CompactBuffer(C, D))
      ((B,E),CompactBuffer(A))
      ((E,C),CompactBuffer(A))
      ((D,A),CompactBuffer(B))
      ((D,E),CompactBuffer(A))
      ((E,B),CompactBuffer(A))
      ((A,B),CompactBuffer(C, D))
      ((D,B),CompactBuffer(A))
      ((A,D),CompactBuffer(B))
      ((E,D),CompactBuffer(A))
      ((B,C),CompactBuffer(A))
      ((C,A),CompactBuffer(B))
      ((F,A),CompactBuffer(E))
      ((C,E),CompactBuffer(A))
      ((A,C),CompactBuffer(B))
      ((B,D),CompactBuffer(A))
      ((C,B),CompactBuffer(A))
      ((A,F),CompactBuffer(E))
      ((C,D),CompactBuffer(B, A))
      ((D,C),CompactBuffer(B, A))
     */
    val step4 = step3.groupByKey();

    /*
      基于步骤3的结果，统计每对人员的共同好友的数量。最后显示：
      ((B,A),2)
      ((B,E),1)
      ((E,C),1)
      ((D,A),1)
      ((D,E),1)
      ((E,B),1)
      ((A,B),2)
      ((D,B),1)
      ((A,D),1)
      ((E,D),1)
      ((B,C),1)
      ((C,A),1)
      ((F,A),1)
      ((C,E),1)
      ((A,C),1)
      ((B,D),1)
      ((C,B),1)
      ((A,F),1)
      ((C,D),2)
      ((D,C),2)
     */
    val groupedCount = step3.map(tuple=>(tuple._1, 1)).reduceByKey(_+_)
    groupedCount.foreach(println(_))
  }

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName(RelationshipStatistics.getClass.getSimpleName)
    val sparkContext = new SparkContext(conf)
    RelationshipStatistics.doit(sparkContext)
    sparkContext.stop()
  }
}
