package com.fnst.cn

import org.apache.spark.sql.SparkSession
object ParquetApp {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("ParquetApp").master("local[2]").getOrCreate();

    /**
      * 第一种加载文件方法
      */
    val userDF = spark.read.format("parquet").load("file:///home/hadoop/app/spark-2.2.0-bin-2.6.0-cdh5.7.0/examples/src/main/resources/users.parquet")

    userDF.printSchema()
    userDF.show()

    userDF.select("name","favorite_color").show

    userDF.select("name","favorite_color").write.format("json").save("file:///home/hadoop/tmp/jsonout")

    /**
      * 第2种加载文件方法
      */
    spark.read.load("file:///home/hadoop/app/spark-2.2.0-bin-2.6.0-cdh5.7.0/examples/src/main/resources/users.parquet").show

    /**
      * 第3种加载文件方法
      */
    spark.read.format("parquet").option("path","file:///home/hadoop/app/spark-2.2.0-bin-2.6.0-cdh5.7.0/examples/src/main/resources/users.parquet").load().show

    /**
      * sql
      */
    spark.read.format("jdbc").option("url", "jdbc:mysql://localhost:3306/default").option("dbtable", "TBLS").option("user", "root").option("password", "root").option("driver", "com.mysql.jdbc.Driver").load()
    spark.stop()

  }

}
