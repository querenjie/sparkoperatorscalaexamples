package sql

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._

/**
  * 这是一个自定义的UDAF类，作用是分组统计输入字符串的个数
  */
class StringGroupCount extends UserDefinedAggregateFunction {
  //输入数据的类型
  override def inputSchema: StructType = {
    StructType(Array(StructField("str", StringType, true)))
  }

  //中间结果的类型
  override def bufferSchema: StructType = {
    StructType(Array(StructField("count", IntegerType, true)))
  }

  //最后返回的数据类型
  override def dataType: DataType = IntegerType

  //永远设为true
  override def deterministic: Boolean = true

  //初始值
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = 0
  }

  //局部累计
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    buffer(0) = buffer.getAs[Int](0) + 1
  }

  //全局累加
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1(0) = buffer1.getAs[Int](0) + buffer2.getAs[Int](0)
  }

  //用于更改返回数据的样子
  override def evaluate(buffer: Row): Any = {
    buffer.getAs[Int](0)
  }
}
