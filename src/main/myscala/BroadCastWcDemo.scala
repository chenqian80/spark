import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by chen on 2018/1/11.
  */
object BroadCastWcDemo {
  def main(args: Array[String]): Unit = {
    //1.构建spark上下文
    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("BroadCastWcDemo")
    val sc=SparkContext.getOrCreate(conf)
    //构建一个广播
    val list =List(",","!",".","*","...",":")
    val broadCastList: Broadcast[List[String]] = sc.broadcast(list)
    //2.数据的读取 -->形成RDD
    val path = "data/halibote.txt"
    val rdd = sc.textFile(path)
    //3.数据处理
    /**
      * 有两种过滤方法;
      * -1.当这个单词,包含了特殊字符,我们就直接到这个单词去掉
      * -2.当这个单词,包含了特殊字符,把这个特殊字符去掉
      */

    //方法一
    val wordcountRDD: RDD[(String, Int)] = rdd
      .flatMap(_.split(" "))
      //list.exists(ch => word.contains(ch))  判断这个word当中是否包含list当中的任意一个字符
      .filter(word => word.nonEmpty && !broadCastList.value.exists(ch => word.contains(ch)))
      .map(word => (word,1))
      .reduceByKey(_ + _)

    val wordcountRDD1: RDD[String] = rdd.flatMap(_.split(" "))
      .filter(word => word.nonEmpty && !broadCastList.value.exists(ch =>word.contains(ch))
       &word.endsWith("b")
      )
    //方法二
    val WordcountRDD2= rdd.flatMap(_.split(" "))
        .filter(word=>word.nonEmpty)
           .map(word=>{
             //a就是每一次的word单词,b就是list遍历出来的每一个特殊字符
             broadCastList.value.foldLeft(word)((a,b) =>a.replace(b," "))
           }).map((_,1))
          .reduceByKey(_+_)




    wordcountRDD1.foreach(println)

  }

}
