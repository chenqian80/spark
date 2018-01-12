package other

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by ibf on 2018/1/7.
 */
object UDFandUDAFSparkSQL {
  def main(args: Array[String]) {
    //1、构建spark上下文
    val conf = new SparkConf()
      .setAppName("RDD2DataFrame")
      .setMaster("local")
    val sc = SparkContext.getOrCreate(conf)
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    //UDF定义
    //保留小数点后两位的方法format
    sqlContext.udf.register("format_double",(value:Double) =>{
      import java.math.BigDecimal
      val bd = new BigDecimal(value)
      //四舍五入
     val t: Double =  bd.setScale(2,BigDecimal.ROUND_HALF_UP).doubleValue()
      t
    })
    //1、创建数据
    sc.parallelize(Array(
      (1,1231),
      (1,1234),
      (1,43435),
      (1,23443),
      (2,34354),
      (2,3423),
      (2,123),
      (3,23423),
      (3,7743),
      (4,12312),
      (4,4545),
      (5,343),
      (5,23426)
    )).toDF("id","sal").registerTempTable("tmp_emp")
  }
}
