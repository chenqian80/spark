package core

import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

/**
 * Created by ibf on 2018/1/3.
 */
object aggreTest {

  def main(args: Array[String]) {

    //1、创建spark上下文
    val conf = new SparkConf()
      .setMaster("local[4]")
      .setAppName("aggTest")
    val sc = SparkContext.getOrCreate(conf)

    val pairRdd = sc.parallelize(List( ("cat",1), ("cat", 1), ("mouse", 1),("cat", 1), ("dog", 1), ("mouse", 1)))
    val resultRdd: RDD[(String, Int)] = pairRdd.aggregateByKey(4,new HashPartitioner(2))(Math.max(_,_),_+_)
    resultRdd.foreachPartition(itr => itr.foreach(println))
    resultRdd.unpersist()

  }
}
