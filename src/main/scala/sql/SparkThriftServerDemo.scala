package sql

import java.sql.{ResultSet, PreparedStatement, DriverManager}

/**
 * Created by ibf on 2018/1/6.
 */
object SparkThriftServerDemo {
  def main(args: Array[String]) {
      //1、给定JDBC的四要素，url，用户名，密码，驱动包

    //1.1 Driver
     val driver = "org.apache.hive.jdbc.HiveDriver"
    //加载driver当中的方法对象到当前的JVM中，以便后续使用
     Class.forName(driver)

    val (url,user,password) = ("jdbc:hive2://bigdata-01:10000","beifeng","123456")
    //通过驱动包的manager获得spark的hive2的连接
    val conn = DriverManager.getConnection(url,user,password)


    val sql1 = "select * from class18.emp a join class18.dept b on a.deptno=b.deptno"
    //通过连接去创建一个执行sql的句柄（对象）
    val pstmt: PreparedStatement = conn.prepareStatement(sql1)
    //通过执行sql的对象来执行sql，返回数据集（ResultSet）
    val rs: ResultSet = pstmt.executeQuery()
    //循环遍历数据集当中的数据，做打印
    var  i= 0
    while (rs.next()){
      i = i+1
      println(s"第${i}个员工,员工ID：${rs.getInt(1)}-----员工姓名：${rs.getString(2)}------员工薪资：${rs.getDouble(6)}")
    }

  println("==========================================")
  //    查看不同部门的平均薪水
    //使用问号，就防止数据泄露，防止sql注入
    val sql2 =
      """
        |select
        |deptno,AVG(sal) as avg_sal
        |from class18.emp
        |group by deptno
        |having avg_sal > ?
      """.stripMargin
    val pstmt2 = conn.prepareStatement(sql2)
    pstmt2.setInt(1,2000)
    val rs2: ResultSet = pstmt2.executeQuery()

    while (rs2.next()){
      println(s"部门ID：${rs2.getInt(1)} ---------- 平均薪水：${rs2.getDouble(2)}")
    }


  }
}
