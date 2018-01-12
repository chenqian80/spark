package other

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Administrator on 2018/1/3.
  */
object PVAndUV {
  def main(args: Array[String]): Unit = {
    val conf =new SparkConf().setMaster("local[4]").setAppName("PVAndUVSparkCore")
    val sc=SparkContext.getOrCreate(conf)
    val path ="data/page_views.data"
    var rdd =sc.textFile(path)
    val index=10
    //处理数据
    val mapRdd=rdd.map(line=>line.split("\t")).filter(
      t=>{
        t.size==7&&t(0).trim.length()>10
      }
    ).map(arr=>{
      val date=arr(0).substring(0,index)
      val pv=arr(1).trim
      val guid=arr(2).trim       //算uv
      (date,pv,guid)
    })
    mapRdd.cache()
    //计算pv
    val pv: RDD[(String, Int)] = mapRdd.map(t =>(t._1,t._2)).filter(t=>t._2.nonEmpty).map(t=>(t._1,1)).reduceByKey(_+_)
    pv.foreach(println)
    //计算Uv
    //    val uv=mapRdd.map(t=>(t._1,t._3)).filter(t=>t._2.nonEmpty).groupByKey().map(t=>{
    //      val  date=t._1
    //      val guids=t._2
    //      (date,guids.toSet.size)
    //    })
    //val uv =mapRdd.filter(k=>{k._3.nonEmpty}).map(t=>((t._1,t._3),1)).reduceByKey((a,b)=>a)
    val uv: RDD[(String, Int)] =mapRdd.filter(k=>{
      k._3.nonEmpty
    }).map(k=>(k._1,k._3)).distinct().map(t=>(t._1,1)).reduceByKey(_+_)
    uv.foreach(println)
    //date,guid相同的作为一个key
    //uv.foreach(println)
    val dataV: RDD[(String, (Option[Int], Option[Int]))] =pv.fullOuterJoin(uv).map(t=>{
      val date=t._1
      val pvOption=t._2._1
      val uvOption=t._2._2
      (date,(pvOption,uvOption))
    })
    dataV.foreach(println)

  }


}