package org.shendiao.sql_practice

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}

import scala.collection.immutable.Nil

object Util {

  def initializeDataViews(sparkSession: SparkSession): Unit={
    val courseSchema = StructType(
      StructField("cid", StringType, false) ::
        StructField("cname", StringType, false) ::
        StructField("tid", StringType, false) :: Nil)
    val studentSchema = StructType(
      StructField("sid", StringType, false) ::
        StructField("sname", StringType, false) ::
        StructField("birth", StringType, false) ::
        StructField("sex", StringType, false) :: Nil)
    val teacherSchema = StructType(
      StructField("tid", StringType, false) ::
        StructField("tname", StringType, false) :: Nil)
    val studentCourseSchema = StructType(
      StructField("sid", StringType, false) ::
        StructField("cid", StringType, false) ::
        StructField("score", DoubleType, false) :: Nil)

    val courseDF = sparkSession.read.schema(courseSchema).option("quote", "'").csv(this.getClass.getResource("/data_source1/course.csv").getPath)
    val studentDF = sparkSession.read.schema(studentSchema).option("quote", "'").csv(this.getClass.getResource("/data_source1/student.csv").getPath)
    val teacherDF = sparkSession.read.schema(teacherSchema).option("quote", "'").csv(this.getClass.getResource("/data_source1/teacher.csv").getPath)
    val studentCourseDF = sparkSession.read.schema(studentCourseSchema).option("quote", "'").csv(this.getClass.getResource("/data_source1/sc.csv").getPath)

    courseDF.show(false)
    studentDF.show(false)
    teacherDF.show(false)
    studentCourseDF.show(false)

    courseDF.createOrReplaceTempView("course")
    studentDF.createOrReplaceTempView("student")
    teacherDF.createOrReplaceTempView("teacher")
    studentCourseDF.createOrReplaceTempView("sc")

  }

}
