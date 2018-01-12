package other

import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by ibf on 2018/1/11.
 */

case class PerSon(name:String,age:Int,sex:String,salary:Int,deptNo:Int)
case class Dept(deptNo:Int,deptName:String)

object SparkSqlDSLDemo {
  def main(args: Array[String]) {
    //1、上下文创建
    val conf = new SparkConf()
        .setMaster("local[*]")
        .setAppName("SparkSqlDSLDemo")
    val sc = SparkContext.getOrCreate(conf)
    //val sqlContext = new HiveContext(sc);
    val sqlContext = new SQLContext(sc);
    import sqlContext.implicits._

    /**
     * 自定义一个方法
     */
    sqlContext.udf.register("sex2Num",(sex:String) => {
      sex.toUpperCase() match {
          //如果是男性，返回0，女性返回1，
          // 如果判断不了，或者说传入的参数既不是M又不是F，就返回-1
        case "M" => 0
        case "F" => 1
        case _ => -1
      }
    })
    sqlContext.udf.register("avg_udaf",Avg_UDAF)

    /**
     * 什么是sparkSql的DSL语法
     * DataFrame当中有一些跟sql很相似的API，select，where。。。。
     * 直接不使用sqlContext.sql  ==》使用DSL语法来替代
     */



    //2、加载数据
    /**
     *  RDD2DataFrame
     *  1、使用case class类型
     *  2、手动给定StructTpye
     */
    val rdd1 =
    sc.parallelize(Array(
      PerSon("张三",21,"M",1235,1),
      PerSon("李四",23,"M",1235,1),
      PerSon("王五",26,"F",1235,1),
      PerSon("赵六",19,"M",1235,1),
      PerSon("小花",21,"F",1225,1),
      PerSon("小华",30,"M",1515,2),
      PerSon("小林",45,"M",1565,2),
      PerSon("leo",17,"M",1865,2),
      PerSon("lili",34,"F",1915,2),
      PerSon("jack",41,"M",2543,3)
    ))
    val rdd2 =
    sc.parallelize(Array(
      Dept(1,"部门1"),
      Dept(2,"部门2"),
      Dept(4,"部门4")
    ))

    val personDataFrame: DataFrame = rdd1.toDF()
    val deptDataFrame = rdd2.toDF()
    personDataFrame.cache()
    deptDataFrame.cache()
    //========================DSL==============================
    println("===========SELECT===========")
    personDataFrame.select("name","age","sex").show
    import org.apache.spark.sql.functions._
    personDataFrame.select(col("name").as("col_name"),col("age"),col("sex")).show
    personDataFrame.select($"name".as("$_name"),$"age",$"sex").show
    personDataFrame.selectExpr("name","age","sex2Num(sex) as sex_num").show

    println("===========WHILE/filter===========")
    personDataFrame.where("age > 20").where("sex = 'M'").where("deptNo = 1").show
    personDataFrame.where("age > 20 AND sex = 'M' AND deptNo = 1").show
    personDataFrame.filter($"age" > 20 && ($"sex" !== "F") && $"deptNo" === 1).show

    println("=================SORT=========================")
    //全局排序，而且是默认升序
    personDataFrame.sort("salary").select("name","salary").show
    //全局排序，降序
    personDataFrame.sort($"salary".desc).select("name","salary").show
    personDataFrame.sort($"salary".desc,$"age".asc).select("name","salary","age").show
    //order by底层调用的是sort，所以也不能解决分区并排序
    personDataFrame.repartition(5).orderBy($"salary".desc,$"age".asc).select("name","salary","age").show

    //局部排序（在各个分区中做排序）
    println("=================局部排序=========================")
    personDataFrame.repartition(5).sortWithinPartitions($"salary".desc,$"age".asc)
      .select("name","salary","age").show

    //group by聚合操作
    println("===================GROUPBY============================")
    //select avg(salary),sex from emp group by sex;
    /**
     * M      1232
     * F      1342
     */
    personDataFrame.groupBy("sex")
      //这里会有一个问题，当同时对一个字段做聚合操作的时候，就会覆盖之前的值
      .agg(
        "salary" -> "avg"
      ).show
    personDataFrame.groupBy("sex")
      .agg(
        "salary" -> "sum"
      ).show
    personDataFrame.groupBy("sex")
      .agg(
        "salary" -> "avg",
        "salary" -> "sum"
      ).show

    /**
     * 出现的问题：当同时对一个字段做聚合操作的时候，就会覆盖之前的值
     * 解决方法：
     *  1、使用Colunm的方式，给定别名
     *  2、直接使用sql就可以
     */
    println("===================给定别名============================")
    personDataFrame.groupBy("sex")
      .agg(
        avg("salary").as("avg"),
        sum("salary").as("sum")
      ).show
    personDataFrame.registerTempTable("person_tmp")
    sqlContext.sql("select sex,avg(salary) as avg,sum(salary) as sum from person_tmp group by sex").show
    /**
     * 使用Colunm的方式，给定别名，但是万一是我们自定义的方法
     * 就不能使用这种类型的解决方法
     */
    personDataFrame.groupBy("sex")
      .agg(
        "salary" -> "avg_udaf"
      ).show

    //limit
    println("==============LIMIT==================")
    personDataFrame.limit(2).show
  }
}
