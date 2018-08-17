package operators

import com.google.gson.Gson
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Transformation
  * map
  * 语法（scala）：
  * def map[U: ClassTag](f: T => U): RDD[U]
  * 说明：
  * 将原来RDD的每个数据项通过map中的用户自定义函数f映射转变为一个新的元素
  */
object MapExample {
  val doit = {
    val conf = new SparkConf().setMaster("local").setAppName(MapExample.getClass.getSimpleName)
    val sc = new SparkContext(conf)

    val datas : Array[String] = Array(
      "{'id':1,'name':'xl1','pwd':'xl123','sex':2}",
      "{'id':2,'name':'xl2','pwd':'xl123','sex':1}",
      "{'id':3,'name':'xl3','pwd':'xl123','sex':2}"
    )

    val datasRDD: RDD[User] = sc.parallelize(datas).map(v => {
      new Gson().fromJson(v, classOf[User])
    })

    datasRDD.foreach(user => println(
      "id:" + user.id + "\tname:" + user.name +
      "\tpwd:" + user.pwd +
      "\tsex:" + user.sex
    ))

    sc.stop()
  }


  def main(args: Array[String]): Unit = {
    MapExample.doit
  }
}

class User {
  var id: Int = 0
  var name: String = ""
  var pwd: String = ""
  var sex: Int = 0
}
