import java.sql.{DriverManager, PreparedStatement}

/**
  * Created by chen on 2018/1/11.
  */
object SparkThrift {
  def main(args: Array[String]): Unit = {
    //1,给定JDBC的四要素,url,用户名,密码,驱动包
    //1.1Driver
    var driver = "org.apache.hive.jdbc.HiveDriver"
    //加载driver当中的方法对象到当前JVM中,以便后续使用
    Class.forName(driver)
    val (url, user, password) = ("jdbc:hive2://hadoop-senior.ibeifeng.com:10000", "beifeng", "123456")
    val conn = DriverManager.getConnection(url, user, password)

    val sql1 = "select * from emp a join dept b on a.deptno=b.deptno"
    //通过一个连接去创建一个执行sql的句柄
    val pstmt: PreparedStatement = conn.prepareStatement(sql1)
    val rs = pstmt.executeQuery()
    //循环遍历数据集当中的数据,做打印
    var i = 0
    while (rs.next()) {
      i = i + 1
      println(s"第${i}个员工,员工ID:${rs.getInt(1)}-----员工姓名:${rs.getString(2)}---员工薪资:${rs.getDouble(6)}")

    }
    println("------------------------")
    //查看不同部门的平均薪水
    //使用问号,就防止数据泄露,防止sql注入
    val sql2 =
    """
        |select
        |deptno,AVG(sal) as avg_sal
        |from emp
        |group by deptno
        |having avg_sal>?
        |
      """.stripMargin
    val pstmt2 = conn.prepareStatement(sql2)
    pstmt2.setInt(1,2000)
    val rs2 = pstmt2.executeQuery()
    while (rs2.next()){
      println(s"部门ID：${rs2.getInt(1)} ---------- 平均薪水：${rs2.getDouble(2)
      (
      )}")
  }


}
  }
