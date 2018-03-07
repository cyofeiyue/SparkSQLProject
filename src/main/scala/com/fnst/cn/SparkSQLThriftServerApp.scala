package com.fnst.cn

import java.sql.DriverManager

object SparkSQLThriftServerApp {
  def main(args: Array[String]): Unit = {
    Class.forName("org.apache.hive.jdbc.HiveDriver");

    val  conn = DriverManager.getConnection("jdbc:hive2://hadoop000:10000","hadoop","");
    val pstmt = conn.prepareStatement("select empno,ename,hiredate from emp");

    val rs = pstmt.executeQuery();
    while(rs.next()){
      println("empno:" + rs.getInt("empno") +
      ",ename:" + rs.getString("ename") +
      ",hiredate:" + rs.getString("hiredate"))
    }

    rs.close();
    pstmt.close();
    conn.close();

  }

}
