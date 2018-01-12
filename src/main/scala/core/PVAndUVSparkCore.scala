package core

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
    //.set("spark.memory.fraction","0.8")
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
    /**
     * 当天数和guid这两个值，完全相同的时候，我们才认为他是同一个需要被去重的guid
     * (date,guid)
     * 考虑：如何去重
     * 方法一：是否有保证值唯一的集合（Set）
     * 最原始，最暴力的方法
     * 弊端：假设数据量特别大
     */
    /*val uv: RDD[(String, Int)] = mapredRDD
      .map(t => {
      //将数据中的url字段去除，只保留date和guid
      (t._1,t._3)
      }).filter(t =>t._2.nonEmpty)
      // RDD[(String, Iterable[String])]
      // (date,itr((guid1),(guid2),(guid1).....)
      .groupByKey()
      .map(t =>{
      val date = t._1
      val guids = t._2
      (date,guids.toSet.size)
    })*/
/*    /**
     * 方法二：如何去重
     * 考虑下，是否可以将 (date,guid)直接作为key
     */
    val uv = mapredRDD
      .filter(t => t._3.nonEmpty)
      .map(t => {
      //将数据中的（date，guid）作为key，给定一个具体的value
      val key: (String, String) = (t._1,t._3)
      val value = 1
      (key,value)
    })
    //如果我对((String, String), Int)做聚合，可以得到什么？
      //guid到底是什么值，你需要知道吗？每个guid统计的count值，需要知道吗
    .reduceByKey((a,b) => a)
    // 按照date分组，reducebykey
    .map(t =>{
      val key = t._1
      val count =t._2  //这个值我是不需要的

      val date = key._1
      val guid = key._2 //这个guid我也可以不不拿到下一次计算当中

      (date,1)
    })
    .reduceByKey(_ + _)
      //可以使用count来获取条数
      //但是万一数据有多天，就会把多天的数据全部count
    //.count()*/

    /**
     * rdd是否也有去重的方法呢？
     * .distinct()
     */
    val uv: RDD[(String, Int)] = mapredRDD
      .filter(t => t._3.nonEmpty)
      .map(t => (t._1,t._3))
      .distinct()
      .map(t => (t._1,1))
      .reduceByKey(_ + _)

    /**
     * pv和uv两个RDD怎么做数据合并？
     *
     */

    val fullRDD: RDD[(String, Int, Int)] = pv.fullOuterJoin(uv)
      .map(t =>{
      val date =t._1
      val pvOption = t._2._1
      val uvOption = t._2._2
      (date,pvOption.getOrElse(-1),uvOption.getOrElse(-1))
    })

    //TODO:UV如果实现，是否也可以做优化！
    println("=======pv的实现========")
    pv.foreachPartition(itr => {
      itr.foreach(println)
    })
    println("=======uv的实现========")
    uv.foreachPartition(itr => {
      itr.foreach(println)
    })
    println("=======full join的实现========")
    fullRDD.foreachPartition(itr => {
      itr.foreach(println)
    })

    mapredRDD.unpersist()
    //线程等待
    Thread.sleep(10000000)
  }
}
