package other

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by chen on 2018/1/2.
  */
object wordCount1 {
  def main(args: Array[String]): Unit = {
    //1.构造spark上下文
    //2.conf sc
    val conf = new SparkConf()
      .setMaster("local")
        .setAppName("YC_WC")
    val sc= new SparkContext(conf)
    //2.生成RDD ---先把RDD看成list
    //本地的路径
    val path= "data/word.txt"
    val pathHDFS="hdfs://"
    val rdd:RDD[String] = sc.textFile(pathHDFS)

    /**
      * 方法一:使用list的API完成mr
      */
     val result:RDD[(String,Int)]= rdd.flatMap(_.split(","))
       .filter(_.trim.nonEmpty)
           .map(word=>(word,1))
             .reduceByKey(_+_)





  }

}
