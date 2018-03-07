package com.fnst.cn

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{StringType, StructField, StructType,IntegerType}

object DataFrameRDDApp {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("DataFrameRDDApp").master("local[2]").getOrCreate();
    // Create an RDD of Person objects from a text file
    val testRDD = spark.sparkContext.textFile("C:\\Users\\Administrator\\IdeaProjects\\SparkSQLProject\\spark-warehouse\\test.txt")
    //inferReflection(spark,testRDD)

    program(spark,testRDD)

    spark.stop();

  }

  def inferReflection(spark: SparkSession,testRDD: RDD[String]): Unit = {

    // RDD ==> DataFrame

    // For implicit conversions from RDDs to DataFrames
    import spark.implicits._
    val infoDF = testRDD.map(_.split(",")).map(line => Info(line(0).toInt, line(1), line(2).toInt)).toDF();

    infoDF.show();

    infoDF.filter(infoDF.col("age") > 30).show

    // Register the DataFrame as a temporary view
    infoDF.createOrReplaceTempView("infos")

    // SQL statements can be run by using the sql methods provided by Spark
    spark.sql("select * from infos where age > 30").show();
  }

  def program(spark:SparkSession,testRDD: RDD[String]): Unit = {

    // The schema is encoded in a string
    val schemaString = "id name age"
    // Generate the schema based on the string of schema
    val fields = schemaString.split(" ").map(fieldName => StructField(fieldName,StringType,nullable = true))

    val schema = StructType(fields)


    val structType = StructType(Array(StructField("id",IntegerType,true),
      StructField("name",StringType,true),
      StructField("age",IntegerType,true)))

    // Convert records of the RDD (people) to Rows
    val rowRDD = testRDD.map(_.split(","))
      .map(attributes => Row(attributes(0),attributes(1).trim,attributes(2)))

    val infoDF = spark.createDataFrame(rowRDD,schema)

    infoDF.printSchema()
    infoDF.show()

    infoDF.filter(infoDF.col("age") > 30).show
    infoDF.createOrReplaceTempView("infos")
    spark.sql("select * from infos where age > 30").show()

  }

  case class Info(id: Int, name: String, age: Int)


}
