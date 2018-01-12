package core

import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by ibf on 2018/1/10.
 */
object AccumulatorsWC {
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
    ),6)

    /**
     * 和之前的mapreduce一样，需要一个计数器
     * Input多少条数据
     * Output多少条数据
     */
  /*  println("输入"+rdd.count()+"条数据")
    println("输出"+rdd.flatMap(_.split(",").filter(!_.isEmpty)).toArray().toSet.size+"条数据")*/

    val inputRecords =  sc.accumulator(0,"Input Record Size")
    val outputRecords =  sc.accumulator(0,"output Record Size")
    /**
     * 累加器是共享变量，
     *  广播变量，只能在Driver中构建，在executor中被使用，是一个只读的值
     *  累加器，只能在Driver中构建，在executor中被使用，是一个只写的值，
     *    1、只能在Driver当中查看他的值
     *    2、在各自的executor的task当中做累加，你的task越多，累加器越多，在各个task运算结束之后
     *      会将值全部返回到Driver，进行最后的累加，得到累加的值
     *      (默认的实现：只能有基本数据类型+String+二元组)
     */

    val rddResult= rdd
      .flatMap(line =>{
      inputRecords+=(1)
      line.split(",")
      .filter(!_.isEmpty)})
      .map(word =>(word.trim,1))
      .reduceByKey(_ + _)
      .foreachPartition(iter => {
      //数据累加


      /**
       * java.lang.UnsupportedOperationException: Can't read accumulator value in task
       *    println("======================="+outputRecords.value)
       */
      iter.foreach(word => {
        outputRecords+=(1)
        println(word)
      })
    })

    println(s"Input Record Size:${inputRecords.value}")
    println(s"output Record Size:${outputRecords.value}")

    Thread.sleep(1000000)
  }
}
