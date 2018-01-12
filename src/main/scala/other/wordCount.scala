package other

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object wordCount {
  def main(args: Array[String]) {
    //1、构建spark的上下文
    //conf  sc
    val conf = new SparkConf()
 //    .setMaster("local")
      .setAppName("YS_WC")
    val sc = new SparkContext(conf)

    //2、生成RDD  --- 先把RDD看成list
    //本地的路径
    val path = "data/word.txt"
    val pathHDFS = "hdfs://192.168.202.100:8020/text.txt"
    val rdd: RDD[String] = sc.textFile(pathHDFS)

    /**
      *  方法一：使用list的API完成的mr
      *
      */
    /* val result: RDD[(String, Int)] = rdd.flatMap(_.split(","))
       .filter(_.trim.nonEmpty)
       //当做了过滤条件之后，如果数据过滤40%
       //记得执行重分区操作
       //.repartition(200)   .cXXX
       .map(word =>(word,1))
       .groupBy(k => k._1)
       .map(t => (t._1,t._2.size))*/

    /**
      * 方法二：使用RDD的API完成聚合，在做wc的时候推荐使用
      *  reduceByKey和aggregateByKey来代替groupByKey的操作
      *  因为前者会做combiner操作
      *  相当于二次聚合
      */
    val result: RDD[(String, Int)] = rdd.flatMap(_.split(" "))
      .filter(_.trim.nonEmpty)
      //当做了过滤条件之后，如果数据过滤40%
      //记得执行重分区操作
      //.repartition(200)   .cXXX
      .map(word =>(word,1))
      .reduceByKey(_+_)

    val top3_1 =  result
      .sortBy(t => t._2,ascending = false)
      .take(3)
    val top3_2: Array[(String, Int)] =  result
      .map(t => (t._2, t))
      .sortByKey(ascending = false)
      .map(t => t._2).take(3)

    println("使用sortBy的方式得到的数据")
    top3_1.foreach(println)
    println("使用sortByKey的方式得到的数据")
    top3_2.foreach(println)
    //3、可以做一个打印（保存HDFS），方法有多种
    //result.collect
    //正常写代码的时候，推荐使用foreachPartition
    result.foreachPartition(itr =>{
      itr.foreach(println)
    })

    //一般来说保存还是存在HDFS上的情况比较多
    result.saveAsTextFile(s"result/rdd_wc/${System.currentTimeMillis()}")
    //result.foreach(println)

    //做一个线程等待，方便去4040页面查看信息
    Thread.sleep(100000000)
  }
}
