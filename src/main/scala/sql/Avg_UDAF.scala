package sql

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._

/**
 * Created by ibf on 2018/1/7.
 */
object Avg_UDAF extends UserDefinedAggregateFunction {
  /**
   * 自定义的聚合函数，必须接受多条数据的参数/值,把这些值做运算
   *   必须要有个内存区（buffer）这个区域存储上一次计算的值，这一次的数据，在和这个值做运算
   *   更新buffer的值，等待下一次的运算
   */
  /**
   * 给定UDAF函数的输入参数的类型（schema）
   * 输入的数据的类型
   * @return
   */
  override def inputSchema: StructType = {
    StructType(Array(
      StructField("iv",DoubleType)
    ))
  }

  /**
   * 对于每一条输入的数据（相同分组的），更新buffer中的数值
   * @param buffer 上一次数据运算/聚合之后得到的数值
   * @param input 当前输入的数据
   */
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    //1、获取当前输入的数据的值
    val iv = input.getDouble(0)

    //MutableAggregationBuffer这个对象，是我们用来更新buffer中的数值的对象
    //是个抽象类，里面只有一个update方法
    //2、获取缓存区当中的数据，（数据其实是上一次聚合得到的值）
    val tv = buffer.getDouble(0)
    val tc = buffer.getInt(1)
    //3、把当前的值累加到上一次的值中，并且更新到缓存区当中
    buffer.update(0, iv + tv)
    buffer.update(1,1+tc)

  }

  /**
   * 指定缓存区中的数据类型
   * 存储两个值
   * 第一个值是sum，把每个人的工资做一个相加（根据id做分区）
   * 第二个值是计算当前分区的总人数
   * @return
   */
  override def bufferSchema: StructType = {
    StructType(Array(
      StructField("tv",DoubleType),
      StructField("tc",IntegerType)
    ))
  }

  /**
   * 当两个分区的结果需要进行合并的时候，会调用该merge方法
   * 更新的时候：是应该更新buffer1的值，还是更新buffer2的值？
   * 应该把buffer2当中的值，更新到buffer1当中
   * @param buffer1
   * @param buffer2
   */
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    //1、获取buffer1的值
      val tv1 = buffer1.getDouble(0)
      val tc1 = buffer1.getInt(1)

    //2、获取buffer2的值
     val tv2 = buffer2.getDouble(0)
    val tc2 = buffer2.getInt(1)

    //3、把buffer2中的值和buffer1的值做聚合，更新到buffer1当中
    buffer1.update(0,tv1+tv2)
    buffer1.update(1,tc1+tc2)
  }

  /**
   * 对缓存区当中的数值做初始化
   * @param buffer
   */
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    //这个只是sum的sal值的初始值
    buffer.update(0,0.0)
    //这个只是count各个部门的人数的值的初始值
    buffer.update(1,0)
  }

  /**
   * 通过这个值设定，给定相同的数据参数，是否会返回相同的数据
   * 通常情况下，都直接给定TRUE
   * @return
   */
  override def deterministic: Boolean = {
      true
  }

  /**
   * evaluate是用来做计算的，最终的buffer如果做计算
   * @param buffer
   * @return
   */
  override def evaluate(buffer: Row): Any = {
    val tv = buffer.getDouble(0)
    val tc = buffer.getInt(1)

    tv/tc
  }

  /**
   * 返回的数据类型是double类型
   * @return
   */
  override def dataType: DataType = {
    DoubleType
  }
}
