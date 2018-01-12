package core

import org.apache.spark.{SparkConf, SparkContext}

import scala.util.Random

/**
 * Created by ibf on 2017/12/24.
 */
object GroupSortTonN {
  def main(args: Array[String]) {
    //1、构建spark上下文
    val conf = new SparkConf()
        .setMaster("local[*,4]")
        .setAppName("GroupSortTonN")
    val sc = SparkContext.getOrCreate(conf)

    //2、读取数据
    val path = "data/groupsort.txt"
    val rdd = sc.textFile(path)
    val k = 3
    //3、处理数据
    /**
     * 需要把单词相同的放到同一个分区里，进行分区中的排序（不是全局排序了）
     *  数据是以" "（空格）为分隔符的
     */
    //方法一：
    // 直接使用list的sorted排序方法
    //但是如果数据量大，对list来说压力也不小
    /*val resultRDD = rdd
      .map(_.split(" "))
      .filter(arr => arr.length == 2)
      .map(arr => (arr(0),arr(1).toInt))
      //将单词相同的数据分到不同组中，接下来考虑如何排序
      .groupByKey()
      .flatMap{
        case(item1,itr) =>{
          val topK = itr
                .toList
                .sorted
            //排完顺序之后，数据按照从小到大排序，使用takeRight从右到左拿3个数据
                .takeRight(k)
          topK.map(item2 => (item1,item2))
        }
      }*/
    /**
     * 方法二：（两阶段聚合）
     * 假设数据有倾斜，某一个单词的数据量特别的大
     * 造成某个task执行时间过长
     * 可以使用局部聚合，然后再进行全局聚合
     * 注意：这里可能会遇到一个序列化的问题
     * 如果在外部构建
     * val i = Random.nextInt(100)
     * 可能会报错
     * 这个类型的优化，并不是优化排序方式，是优化数据倾斜
     */
   val resultRDD = rdd
      .map(_.split(" "))
      .filter(arr => arr.length == 2)
      .map(arr => ((Random.nextInt(100),arr(0)),arr(1).toInt))
      .groupByKey()
      //将key中的随机值去掉
      .flatMap{
        case((_,item),iter) => {
          val topK = iter
            .toList
            .sorted
            .takeRight(3)

          topK.map(item2 => (item,item2))
        }
      }
      //再做一次全局的groupby
      .groupByKey()
      .flatMap{
      case(item,iter) => {
        val topK = iter
          .toList
          .sorted
          .takeRight(3)
        topK.map(item2 => (item,item2))
      }
    }

    /**
     * 当你想使用groupByKey的时候，
     * 思考下是否可以使用reduceByKey和aggregateByKey来解决你的问题
     *
     * 做数据倾斜的时候，我们会对整个日志文本做抽样（SampleXXX），
     * 做wc，判断哪个key的数据最多，task时间很久的，就是这几个key造成的
     * 如果这个key可以丢弃，可以filter,如果不能丢弃，加随机值，将这个key的数据分到不同的task上面处理
     */
   /* val resultRDD = rdd
      .map(_.split(" "))
      .filter(arr => arr.length == 2)
      .map(arr => (arr(0),arr(1).toInt))
      .aggregateByKey(ArrayBuffer[Int]())(
        (u,v) =>{
          //将v和u合并，返回一个新对象u
          u += v
          u.sorted.takeRight(3)
        },(u1,u2) =>{
      //将u2里面的数据加到u1当中，最后返回一个新的排过序的u对象
        u1 ++= u2
        u1.sorted.takeRight(3)
    })*/

    resultRDD.foreachPartition(itr => {
      itr.foreach(println)
    })

    Thread.sleep(10000000)
  }
}
