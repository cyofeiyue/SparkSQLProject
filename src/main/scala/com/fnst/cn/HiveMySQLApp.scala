package com.fnst.cn

import org.apache.spark.sql.SparkSession
object HiveMySQLApp {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("HiveMySQLApp").master("local[2]").getOrCreate()
    // 加载Hive表数据
    val hiveDF = spark.table("emp")

    // 加载MySQL表数据
    val mysqlDF =  spark.read.format("jdbc").option("url", "jdbc:mysql://hadoop000:3306").option("dbtable", "test1.DEPT").option("user", "root").option("password", "root").option("driver", "com.mysql.jdbc.Driver").load()

    val resultDF = hiveDF.join(mysqlDF,hiveDF.col("deptno") === mysqlDF.col("DEPTNO"))
    resultDF.show()

    resultDF.select(hiveDF.col("empno"),hiveDF.col("ename"), mysqlDF.col("deptno"), mysqlDF.col("dname")).show

    spark.stop()

  }

}
