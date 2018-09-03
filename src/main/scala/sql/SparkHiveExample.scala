package sql

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.hive.HiveContext

/**
  * Spark访问Hive的例子。
  * 要运行的话必须先打包成jar，然后在linux上运行，原因是hive-site.xml在linux上。
  * hive-site.xml文件从hive中复制到spark中。
  * 两个文本文件和用maven package打包好的jar都放在linux上的某个目录下。
  * 在linux上要启动hadoop、mysql、spark，然后运行以下spark命令：
  * bin/spark-submit --master spark://192.168.1.20:7077 --class sql.SparkHiveExample /usr/local/tmp/sparkoperatorscalaexamples-1.0-SNAPSHOT.jar
  * 其中要特别注意的是：文本文件中的每一行中用','分隔字段的，所以在建表的时候要有row format delimited fields terminated by ','这句话。
  */
object SparkHiveExample {
  private def doit(sparkContext: SparkContext): Unit = {
    val hiveContext = new HiveContext(sparkContext)
    hiveContext.sql("drop table if exists student_infos")
    hiveContext.sql("create table if not exists student_infos (name string, age int) row format delimited fields terminated by ','")
    hiveContext.sql("load data local inpath '/usr/local/tmp/student_infos.txt' into table student_infos")

    hiveContext.sql("drop table if exists student_scores")
    hiveContext.sql("create table if not exists student_scores (name string, score int) row format delimited fields terminated by ','")
    hiveContext.sql("load data local inpath '/usr/local/tmp/student_scores.txt' into table student_scores")

    val goodStudentDataset = hiveContext.sql("select si.name, si.age, ss.score from student_infos si join student_scores ss on si.name=ss.name and ss.score>=80")
    goodStudentDataset.show()
    //结果存入hive表中
    hiveContext.sql("drop table if exists good_student_infos")
    goodStudentDataset.write.saveAsTable("good_student_infos")
  }

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(SparkHiveExample.getClass.getSimpleName)
    val sparkContext = new SparkContext(conf)
    SparkHiveExample.doit(sparkContext)
    sparkContext.stop()
  }
}
