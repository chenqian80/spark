package sql

import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types._
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by ibf on 2018/1/6.
 */
object ReadCSVSparkSQL {
  def main(args: Array[String]) {
    //1、构造spark上下文
    val conf = new SparkConf()
        .setMaster("local[*]")
        .setAppName("ReadCSVSparkSQL")
        .set("spark.sql.shuffle.partitions","5")
    val sc = SparkContext.getOrCreate(conf)
    //如果下面的代码使用到了hive的窗口函数，那么这里的对象就不能使用SQLContext
    //使用HiveContext的时候要注意，增加方法区内存大小-XX:PermSize=128M -XX:MaxPermSize=256M
    //如果不使用hive的窗口函数，其实直接使用SQLContext就够了
    val sqlContext = new HiveContext(sc)

    //2、读取CSV数据

    //2.1 给定数据的schema信息
    val schema = StructType(Array(
      StructField("tid",IntegerType),
      StructField("lat",StringType),
      StructField("lon",StringType),
      StructField("time",StringType)
    ))
    val path = "data/taxi.csv"
    //2.2将数据注册成一张临时表，为后续操作提供表数据
     sqlContext.read
      .format("com.databricks.spark.csv")
      .option("header","false")
     //是否没有给定数据的schema信息（字段的名称，字段类型）
      .schema(schema)
      .load(path)
      .registerTempTable("tmp_taxi")


    //我想用hive的row_number来解决问题
    /**
     *  row_number() over(partition by XXX order by XXX [desc/asc]) as rnk
     */

    sqlContext.sql(
      """
        |SELECT
        |tid,substring(time,0,2) as hour
        |FROM
        |tmp_taxi
      """.stripMargin)
      .registerTempTable("tmp_tid_hour")

    //group by tid,hour
    sqlContext.sql(
      """
        |SELECT
        |tid, hour, count(1) as count
        |FROM
        |tmp_tid_hour
        |GROUP BY
        |tid,hour
      """.stripMargin)
    .registerTempTable("tmp_tid_hour_count")

  //按小时分区，按照count做排序
    sqlContext.sql(
      """
        |SELECT
        |tid, hour,count,
        |ROW_NUMBER() OVER (PARTITION BY hour ORDER BY count desc) as rnk
        |FROM
        |tmp_tid_hour_count
      """.stripMargin)
      .registerTempTable("tmp_tid_hour_count_rnk")

    //求top5
    sqlContext.sql(
      """
        |SELECT
        |tid, hour,count, rnk
        |FROM
        |tmp_tid_hour_count_rnk
        |WHERE rnk <= 5
      """.stripMargin)
      .repartition(1)

      //可以直接缓存DataFrame
      // .cache()
      //.persist()
      //也可以通过sqlContext来保存表
      //sqlContext.cacheTable("tmp_tid_hour_count_rnk")
      .write
      .format("org.apache.spark.sql.hive.orc")
      .option("header","true")
      .mode(SaveMode.Overwrite)
      .save(s"result/sql/${System.currentTimeMillis()}")
    //线程等待，查看4040页面的结果
    Thread.sleep(100000)

  }
}
