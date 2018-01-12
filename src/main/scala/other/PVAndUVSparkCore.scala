package other

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by ibf on 2017/12/23.
 */
object PVAndUVSparkCore {
  def main(args: Array[String]) {
    //1、创建spark上下文
    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("PVAndUVSparkCore")
    val sc = SparkContext.getOrCreate(conf)

    //2、读取文件
    val path = "data/page_views.data"
    val rdd: RDD[String] = sc.textFile(path)

    //3、处理数据
    val index = 10
    val mapredRDD: RDD[(String, String, String)] = rdd
      .map(line => line.split("\t"))
      .filter(arr =>{
      //每次取到的一行的值要求长度为7，第一个字段时间必须要完整
          arr.length == 7 && arr(0).trim.length() > 10
      })
    //这里可以做重分区操作，如果过滤的数据特别多的话
      .map(arr => {
      val date = arr(0).substring(0,index)
      val url = arr(1).trim  //算PV
      val guid = arr(2).trim //算UV
      //日期，url，guid（相同用户的不同访问操作，guid是相同的）
      (date,url,guid)
    })
    //mapredRDD可能在接下来会被操作多次，所以cache
    mapredRDD.cache()

    //3.1 计算PV
    /**
     * 你不需要的数据，就没有必要拿到进行处理
     */
    /*val pv: RDD[(String, Int)] = mapredRDD
      //不需要guid，所以先使用map方法，将guid过滤掉
      .map(t =>(t._1,t._2))
      .filter(t => t._2.nonEmpty)
      .groupByKey()
      .map(t =>{
      val date = t._1
      val urls: Iterable[String] = t._2
      (date,urls.size)
      })*/
      /**
       * PV的优化
       * url的值是什么，我们并不关注
       * 我们需要的只是：url的条数
       * 所以可以直接将字符串换成 int 1
       * 写代码的时候，记得思考什么样的方式能减少内存的消耗
       */

    val pv: RDD[(String, Int)] = mapredRDD
      //不需要guid，所以先使用map方法，将guid过滤掉
      .map(t =>(t._1,t._2))
      .filter(t => t._2.nonEmpty)
        .map(t => (t._1,1))
        .reduceByKey(_ + _)
    val uv: RDD[(String, Int)] =mapredRDD.map(t=>{
      (t._1,t._3)
    }).filter(t=>t._2.nonEmpty)
      //RDD[(String,Interable[string])]
      //(date,itr((guid1),(guid2)))
      .groupByKey()
        .map(t=>{
          val date = t._1
          val guids = t._2
          (date,guids.toSet.size)
        })
    /**
      * 方法二:
      * 考虑下,是否(data,guid)直接为key
      */
    val uv1: Long = mapredRDD
      .filter(t=>t._3.nonEmpty)
        .map(t=>{
          val key=(t._1,t._2)
          val value=1
          (key,value)
        })
          .reduceByKey(_+_)
            .count()


    //TODO:UV如果实现，是否也可以做优化！
    pv.foreachPartition(itr => {
      itr.foreach(println)
    })
    println("--------------uv的实现--------")
    uv.foreachPartition(itr=>{
      itr.foreach(println)
    })

  }
}
