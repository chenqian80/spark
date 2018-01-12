package sql

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by ibf on 2018/1/6.
 */
object BroadCastWC {
  def main(args: Array[String]) {
    //1、构建spark上下文
    val conf = new SparkConf()
        .setMaster("local[*]")
        .setAppName("BroadCastWC")
    val sc = SparkContext.getOrCreate(conf)
    //构建一个广播变量
    val list = List(",","!",".","“","”","—","'","?","*","…",":")
    val broadcastList: Broadcast[List[String]] = sc.broadcast(list)
    //2、数据的读取 --》形成RDD
    val path = "data/halibote.txt"
    val rdd = sc.textFile(path)

    //3、数据处理
    /**
     * 有两种过滤方式：
     *  -1.当这个单词，包含了特殊字符，我们就直接把这个单词去掉！
     *        line.   "asdasd ==》 这两个字符串直接去掉
     *  -2.当这个单词，包含了特殊字符，我们就把这个单词上的特殊字符去掉！
     *        line.   "asdasd ==》 line  asdasd
     */

    /**
     * 这段代码，其实没有使用广播变量！
     */
    /*val wordcountRDD = rdd
          .flatMap(_.split(" "))
      //list.exists(ch => word.contains(ch))  判断这个word当中是否包含list当中的任意一个字符
          .filter(word => word.nonEmpty && !list.exists(ch => word.contains(ch)))
          .map(word => (word,1))
          .reduceByKey(_ + _)*/
    /**
     * 方法一：这里是使用广播变量的方法
     */
     val wordcountRDD = rdd
      .flatMap(_.split(" "))
      //list.exists(ch => word.contains(ch))  判断这个word当中是否包含list当中的任意一个字符
      .filter(word => word.nonEmpty && !broadcastList.value.exists(ch => word.contains(ch)))
      .map(word => (word,1))
      .reduceByKey(_ + _)

    /**
     * 方法二：这里是保留了包含特殊字符的word的方法（把每个word上的特殊字符去掉）
     */
    val wordcountRDD1 = rdd
      .flatMap(_.split(" "))
      .filter(word => word.nonEmpty)
      .map(word =>{
      //a就是每一次的word单词，b就是list遍历出来的每一个特殊字符
      broadcastList.value.foldLeft(word)((a,b) => a.replace(b,""))
    }).map((_,1))
      .reduceByKey(_ + _)


      //.reduceByKey(_ + _)

    //打印rdd当中的数据
    println("=============这里是把包含特殊字符的单词直接过滤掉==================")
    wordcountRDD.foreachPartition(iter => {
      iter.foreach(println)
    })

  }

}
