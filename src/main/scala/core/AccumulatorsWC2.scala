package core

import org.apache.spark.{AccumulableParam, SparkConf, SparkContext}

import scala.collection.mutable
import scala.collection.mutable._

/**
 * Created by ibf on 2018/1/10.
 */


object MapAccumulableParam extends AccumulableParam[Map[String,Int],String]{
  /**
   * 添加一个元素到累加器当中
   * @param r
   * @param t
   * @return
   */
  override def addAccumulator(r: mutable.Map[String, Int], t: String): mutable.Map[String, Int] = {
    r += t -> (1 + r.getOrElse(t,0))
  }

  /**
   * 在Driver当中合并两个不同的累加器
   * @param r1
   * @param r2
   * @return
   */
  override def addInPlace(r1: mutable.Map[String, Int], r2: mutable.Map[String, Int]): mutable.Map[String, Int] = {
    r1.foldLeft(r2)((a,b) =>{
      a += b._1 -> (a.getOrElse(b._1,0) + b._2)
    })

  }

  /**
   * 累加器的初始值
   * @param initialValue
   * @return
   */
  override def zero(initialValue: mutable.Map[String, Int]): mutable.Map[String, Int] = initialValue
}

object AccumulatorsWC2 {
  def main(args: Array[String]) {
    //1、构建spark的上下文
    val conf = new SparkConf()
      .setAppName("AccumulatorsWC")
      .setMaster("local[*]")
    val sc = SparkContext.getOrCreate(conf)

    val rdd = sc.parallelize(Array(
      "hadoop,spark,hbase",
      "",
      "hadoop,hive,hue",
      "spark,spark,spark",
      "hadoop,spark,hbase",
      "hadoop,test,Demo",
      "hadoop,hive,hue",
      "Demo,hive,Demo"
    ), 6)

    //需求：要求通过累加器来实现（不使用reducebykey的API），wordcount程序
     val map = Map[String,Int]()
   val mapAccumulable =  sc.accumulable(Map[String,Int]())(param = MapAccumulableParam)

    rdd.foreachPartition(iter =>{
      iter.foreach(line =>{
        val nline = if(line == null)"" else line
        //对数据进行分割，过滤，转换
        nline.split(",")
             .filter(!_.trim.isEmpty)
              .map(word => {

          mapAccumulable += word
        })
      })
    })

    mapAccumulable.value.foreach(println)

    Thread.sleep(1000000)
  }
}
