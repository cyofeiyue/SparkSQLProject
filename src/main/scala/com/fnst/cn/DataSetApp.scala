package com.fnst.cn

import org.apache.spark.sql.SparkSession

object DataSetApp {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .appName("DataSetApp")
      .master("local[2]")
      .getOrCreate()

    import spark.implicits._

    val path = "C:\\Users\\Administrator\\IdeaProjects\\SparkSQLProject\\spark-warehouse\\sales.csv";
    val df = spark.read.option("header","true").option("inferSchema","true").csv(path)
    df.show()

    // DataFrames can be converted to a Dataset by providing a class. Mapping will be done by name.
    val ds = df.as[Sales]

    ds.map(line => line.itemId).show()


  }

  case class Sales(transactionId:Int,customerId:Int,itemId:Int,amountId:Double)

}
