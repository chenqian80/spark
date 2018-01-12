package sql

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Dataset, Row, DataFrame, SQLContext}
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by ibf on 2018/1/7.
 */
//创建了自己的一个case class类型
case class Person(name:String,age:Int)
object RDD2DataFrame {
  def main(args: Array[String]) {
    //1、构建spark上下文
    val conf = new SparkConf()
        .setAppName("RDD2DataFrame")
        .setMaster("local")
    val sc = SparkContext.getOrCreate(conf)
    val sqlContext = new SQLContext(sc)

    //2、创建数据
   val rdd: RDD[Person] = sc.parallelize(Array(
      Person("lin",16),
      Person("leo",26),
      Person("jack",23),
      Person("jim",13),
      Person("jane",5),
      Person("shark",65)
    ))

    /**
     * rdd转换DataFrame的第一个方法：利用反射机制来确定schema
     *    要求一:rdd中的数据类型必须是case class
     *    要求二：引入sqlContext的隐式转换函数
     */
    import sqlContext.implicits._
    val df: DataFrame = rdd.toDF("pname","age")
    //如果选择传参数的方式，传入的参数个数一定要和原始的属性个数相同
    //val df1: DataFrame = rdd.toDF("pname1","page1","ppp")
    //val df2: DataFrame = rdd.toDF("pname2")
    println("===============df====================")
    df.show

    val rddt = sc.parallelize(Array(
      ("lin",16),
      ("leo",26),
      ("jack",23),
      ("jim",13),
      ("jane",5),
      ("shark",65)
    ))
    println("===============dft====================")
    rddt.toDF("tname1","tage2").show


    /**
     * 方法二：使用SQLContext的createDataFrame来创建一个DataFrame
     *
     * RDD[ROW],StructType
     *
     * ROW类型可以理解为，表当中的一行
     */
    //将普通的数据类型转换成RDD[ROW]
    val rddt2: RDD[Row] = sc.parallelize(Array(
      ("lin",16,3431),
      ("leo",26,32435),
      ("jack",23,2354),
      ("jim",13,5432),
      ("jane",5,3432),
      ("shark",65,6763)
    )).map{
      case (name,age,sal) => {
        Row(name,age,sal)
      }
    }

    //给定schema信息   ==》StructType
    val schema = StructType(Array(
      StructField("name1",StringType),
      StructField("age1",IntegerType),
      StructField("sal1",IntegerType)
    ))

    val df2: DataFrame = sqlContext.createDataFrame(rddt2,schema)
    println("=============方法二，手动给定RDD[Row]和schema信息=================")
    df2.show()

    println("=============Dataset=================")
   val ds: Dataset[(String, Int)] =  rddt.toDS()
    ds.show()
  }
}
