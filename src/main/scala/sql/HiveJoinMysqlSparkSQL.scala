package sql

import java.util.Properties

import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by ibf on 2018/1/6.
 */
object HiveJoinMysqlSparkSQL extends App{
    //使用了lazy字段，变量不会立刻被加载，并消耗内存
    //只有在调用的时候，才会将这个对象创建出来
    lazy val driver = "com.mysql.jdbc.Driver"
    lazy val user = "root"
    lazy val password = "123456"
    lazy val url = "jdbc:mysql://bigdata-01:3306/test"
    lazy val props = new Properties()
    props.put("usr",user)
    props.put("passeword",password)
    //给定保存的hdfs上的路径
    lazy val path = "hdfs://bigdata-01:8020/hivejoinmysql"

    //1、构建spark上下文
    val conf = new SparkConf()
      .setAppName("HiveJoinMysqlSparkSQL")
      .setMaster("local[*]")
    val sc = SparkContext.getOrCreate(conf)
  //HiveContext
    val sqlContext = new SQLContext(sc)


  /**
   * 需求一：使用spark的jdbc的方法往mysql当中存储数据
   * 2、读取hive当中的dept的数据，存储到mysql当中
   */

   sqlContext
      .read
      .table("class18.dept")
      .write
      .mode(SaveMode.Overwrite)
      .jdbc(url,"tb_dept",props)

  /**
   * 需求二：从mysql当中拿取数据
   * 2、使用spark的jdbc的方法，将msyql的数据读取到DataFrame，注册成一张临时表，作为join的表源
   */

  /**
   * 读取jdbc数据的方式，最重要的就是读取出来的数据如何分区
   * 方法一：传入Array[String] 手动给定分区的条件，有多少个String对象，就分多少个区
   *      一般数据分区比较少的情况，我们会采取这样的方式，更加准确
   */
/*  sqlContext
    .read
    .jdbc(url,"tb_dept",Array("deptno < 25","deptno >=25 AND deptno < 28","deptno >=28"),props)*/
  /**
   *
   * 方法二：传入（分区的字段名称+下界+上界+分区格式） 代码会自动帮我们判断每个分区的数据范围，
   * 数据分区比较多的情况，可以采取这样的方式，减少代码量
   */

  sqlContext
    .read
    .jdbc(url,"tb_dept","deptno",10,30,3,props)
    .registerTempTable("dept_mysql")

  //3、执行sql语句形成join的DataFrame
  val joinRDD: DataFrame = sqlContext.sql("select a.*,b.dname,b.loc from class18.emp a join dept_mysql b on a.deptno=b.deptno")
  //因为接下来要做展示和保存，所以可以先缓存下
  joinRDD.cache()
  joinRDD.show
  //3、将这个join的DataFrame保存到HDFS上，数据格式为parquet
  joinRDD.write
    .format("parquet")
    .mode(SaveMode.Overwrite)
    .save(path)
  //顺便也在本地存一份
 val df: DataFrame =  joinRDD.repartition(1)
  df.write
    .format("json")
    .mode(SaveMode.Overwrite)
    .save(s"result/sql/hivejoinmysql/${System.currentTimeMillis()}")
  //使用saveAsTable来保存一张表数据，保存到hive当中
  joinRDD.write
    .mode(SaveMode.Overwrite)
    .saveAsTable("tb_result")

  Thread.sleep(100000)

}
