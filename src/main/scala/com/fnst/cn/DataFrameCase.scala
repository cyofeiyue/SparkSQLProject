package com.fnst.cn
import org.apache.spark.sql.SparkSession

object DataFrameCase {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("DataFrameCase")
      .master("local[2]")
      .getOrCreate()

    val rdd = spark.sparkContext.textFile("C:\\Users\\Administrator\\IdeaProjects\\SparkSQLProject\\spark-warehouse\\student.data");

    import spark.implicits._
    val studentDF = rdd.map(_.split("\\|"))
      .map(line => Student(line(0).toInt,line(1),line(2),line(3)))
      .toDF()

    studentDF.show
    studentDF.show(30,false)

    studentDF.take(10)
    studentDF.first()
    studentDF.head(3)

    studentDF.select("email").show(30,false)

    studentDF.filter("name='' OR name='NULL'").show

    //name以B开头的人
    studentDF.filter("SUBSTR(name,0,1)='B'").show

    //sort
    studentDF.sort(studentDF("name")).show
    studentDF.sort(studentDF("name").desc).show

    studentDF.sort("name","id").show

    studentDF.sort(studentDF("name").asc, studentDF("id").desc).show

    //as alias
    studentDF.select(studentDF("name").as("student_name")).show


    val studentDF2 = rdd.map(_.split("\\|")).map(line => Student(line(0).toInt, line(1), line(2), line(3))).toDF()

    //inner join ===
    studentDF.join(studentDF2, studentDF.col("id") === studentDF2.col("id")).show

    spark.stop()

  }

   case class Student(id: Int, name: String, phone: String, email: String)

}
