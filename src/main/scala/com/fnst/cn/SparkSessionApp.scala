package com.fnst.cn

import org.apache.spark.sql.SparkSession

object SparkSessionApp {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("SparkSessionApp").master("local[2]").getOrCreate();
    val people = spark.read.json("file:///C:\\Users\\Administrator\\IdeaProjects\\SparkSQLProject\\spark-warehouse\\people.json")

    people.show();

    spark.stop();
  }

}
