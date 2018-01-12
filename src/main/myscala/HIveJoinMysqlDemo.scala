/**
  * Created by chen on 2018/1/11.
  */
object HIveJoinMysqlDemo  extends App{
  //使用了lazy字段,字段不会被加载,并消耗内存
  //只有在调用的时候,才会将这个字段创建出来
  lazy val driver = "com.mysql.jdbc.Driver"
  lazy val user = "root"
  lazy val password="123456"
  lazy val url ="jdbc:mysql://"

}
