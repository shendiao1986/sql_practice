package org.shendiao.sql_practice

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{DateType, DoubleType, StringType, StructField, StructType}
import org.apache.spark.sql.functions._
import scala.collection.immutable.Nil

object SQLPractice {

  def main(args: Array[String]): Unit = {

    val sparkSession = SparkSession.builder().master("local[*]").appName("SQL Practice").getOrCreate()
    val courseRDD = sparkSession.read.textFile("/data_file/C.txt").rdd
    val studentRDD = sparkSession.read.textFile("/data_file/S.txt").rdd
    val teacherRDD = sparkSession.read.textFile("/data_file/T.txt").rdd
    val studentCourseRDD = sparkSession.read.textFile("/data_file/SC.txt").rdd

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

    val stdCourseRDD = courseRDD.map(_.split(",").map(_.trim.replace("'", ""))).map(vs => Row(vs(0).toString, vs(1).toString, vs(2).toString))
    val stdStudentRDD = studentRDD.map(_.split(",").map(_.trim.replace("'", ""))).map(vs => Row(vs(0).toString, vs(1).toString, vs(2).toString, vs(3).toString))
    val stdTeacherRDD = teacherRDD.map(_.split(",").map(_.trim.replace("'", ""))).map(vs => Row(vs(0).toString, vs(1).toString))
    val stdStudentCourseRDD = studentCourseRDD.map(_.split(",").map(_.trim.replace("'", ""))).map(vs => Row(vs(0).toString, vs(1).toString, vs(2).toDouble))

    val cDF = sparkSession.createDataFrame(stdCourseRDD, courseSchema)
    val sDF = sparkSession.createDataFrame(stdStudentRDD, studentSchema)
    val tDF = sparkSession.createDataFrame(stdTeacherRDD, teacherSchema)
    val scDF = sparkSession.createDataFrame(stdStudentCourseRDD, studentCourseSchema)

    val sDFStd=sDF.withColumn("birth", to_date(col("birth"),"yyyy-MM-dd"))

    cDF.show()
    cDF.printSchema()
    sDF.show()
    sDF.printSchema()
    tDF.show()
    tDF.printSchema()
    scDF.show()
    scDF.printSchema()

    cDF.createOrReplaceTempView("course")
    sDF.createOrReplaceTempView("student")
    tDF.createOrReplaceTempView("teacher")
    scDF.createOrReplaceTempView("sc")

    //1 查询" 01 "课程比" 02 "课程成绩高的学生的信息及课程分数
    sparkSession.sql("select sc1.sid, sc1.cid, sc1.score score1, sc2.sid, sc2.cid, sc2.score score2 from sc sc1 left join sc sc2 on sc1.sid = sc2.sid where sc1.score>sc2.score and sc1.cid='01' and sc2.cid='02'").show()
    //2 查询同时存在" 01 "课程和" 02 "课程的情况
    sparkSession.sql("select * from (select * from sc where sc.cid='01') as sc1, (select * from sc where sc.cid='02') as sc2 where sc1.sid=sc2.sid").show()
    //3 查询存在"01"课程但可能不存在"02"课程的情况(不存在时显示为 null )
    sparkSession.sql("select * from (select * from sc where sc.cid='01') as sc1 left join sc sc2 on sc1.sid=sc2.sid and sc2.cid='02'").show()
    //4 查询不存在"01"课程但存在"02"课程的情况
    sparkSession.sql("select * from (select * from sc where sc.cid='02') as sc1 left join (select * from sc where sc.cid='01') as sc2 on sc1.sid=sc2.sid where sc2.sid is null").show()
    //5 查询平均成绩大于等于 60 分的同学的学生编号和学生姓名和平均成绩
    sparkSession.sql("select sid, avg(score) from sc group by sid having avg(score) >=60").show()
    //6 查询在 SC 表存在成绩的学生信息
    sparkSession.sql("select distinct s.* from student s join sc sc1 on s.sid=sc1.sid").show()
    //7 查有成绩的学生信息
    sparkSession.sql("select * from student s where s.sid in (select distinct sid from sc)").show()
    //8 查询「李」姓老师的数量
    sparkSession.sql("select count(*) from teacher where tname like '李%'").show()
    //9 查询学过张三老师授课的同学的信息
    sparkSession.sql("select sid from sc sc1 join (select cid from course c where c.tid = (select tid from teacher where tname = '张三')) cc1 on sc1.cid = cc1.cid group by sid").show()
    //10 查询没有学全所有课程的同学的信息
    sparkSession.sql("select s.sid, count(sc1.cid) from student s left join sc sc1 on s.sid=sc1.sid group by s.sid having count(sc1.cid) != (select count(cid) from course) order by s.sid").show()
    //11 查询至少有一门课与学号为" 01 "的同学所学相同的同学的信息
    sparkSession.sql("select sc2.sid from (select * from sc where sid='01') sc1 left join sc sc2 on sc1.cid=sc2.cid and sc2.sid!='01' group by sc2.sid order by sc2.sid").show()
    //12 查询和" 01 "号的同学学习的课程完全相同的其他同学的信息
    sparkSession.sql("select sc1.sid, count(sc1.cid) from (select cid from sc where sid='01') c left join sc sc1 on c.cid = sc1.cid and sc1.sid!='01' group by sc1.sid having count(sc1.cid) = (select count(cid) from sc where sid='01')").show()
    //13 查询没学过"张三"老师讲授的任一门课程的学生姓名
    sparkSession.sql("select * from student s where s.sid not in(select sid from sc sc1 left join course c on sc1.cid = c.cid left join teacher t on c.tid=t.tid where t.tname='张三')").show()
    //14 查询两门及其以上不及格课程的同学的学号，姓名及其平均成绩
    sparkSession.sql("select sid,avg(score) from sc where sc.sid in (select sid from sc where score <60 group by sid having count(sid)>=2) group by sid").show()
    //15 检索" 01 "课程分数小于 60，按分数降序排列的学生信息
    sparkSession.sql("select * from sc where sc.cid='01' and score<60 order by score desc").show()
    //16 按平均成绩从高到低显示所有学生的所有课程的成绩以及平均成绩
    sparkSession.sql("select * from sc sc1 left join (select sid, avg(score) avg_score from sc group by sid) sc2 on sc1.sid=sc2.sid order by avg_score desc").show()
    //17 查询各科成绩最高分、最低分和平均分
    sparkSession.sql("select c.cid, c.cname, max(sc1.score), min(sc1.score), avg(sc1.score), count(sc1.sid) from course c left join (select *, case when score >=60 then 1 else 0 end as a1, case when score >=70 and score <=80 then 1 else 0 end as a2, case when score >=80 and score <=90 then 1 else 0 end as a3, case when score >=90 then 1 else 0 end as a4 from sc) sc1 on c.cid=sc1.cid group by c.cid, c.cname order by count(sc1.sid) desc, cid asc").show()
    sparkSession.sql("select *, case when score >=60 then 1 else 0 end as a1, case when score >=70 and score <=80 then 1 else 0 end as a2, case when score >=80 and score <=90 then 1 else 0 end as a3, case when score >=90 then 1 else 0 end as a4 from sc").show()
    sparkSession.sql("select c.cid, c.cname, max(sc1.score), min(sc1.score), avg(sc1.score), count(sc1.sid), sum(a1)/count(sid),sum(a2)/count(sid),sum(a3)/count(sid),sum(a4)/count(sid) from course c left join (select *, case when score >=60 then 1 else 0 end as a1, case when score >=70 and score <=80 then 1 else 0 end as a2, case when score >=80 and score <=90 then 1 else 0 end as a3, case when score >=90 then 1 else 0 end as a4 from sc) sc1 on c.cid=sc1.cid group by c.cid, c.cname order by count(sc1.sid) desc, cid asc").show()
    //18 按各科成绩进行排序，并显示排名， Score 重复时保留名次空缺
    sparkSession.sql("select sc.*, dense_rank() over(partition by cid order by score desc) rk from sc").show()
    //19 按各科成绩进行行排序，并显示排名， Score 重复时合并名次
    sparkSession.sql("select sc.*, rank() over(partition by cid order by score desc) rk from sc").show()
    //20 查询学生的总成绩，并进行排名，总分重复时保留名次空缺
    sparkSession.sql("select t1.*, dense_rank() over(order by t1.total_score desc) rk from (select sid, sum(score) total_score from sc group by sid) t1").show()
    //21 查询学生的总成绩，并进行排名，总分重复时保留名次空缺
    sparkSession.sql("select t1.*, rank() over(order by t1.total_score desc) rk from (select sid, sum(score) total_score from sc group by sid) t1").show()
    //22 统计各科成绩各分数段人数：课程编号，课程名称，[100-85]，[85-70]，[70-60]，[60-0] 及所占百分比
    sparkSession.sql("select sid, cid, case when score >=0 and score <60 then 1 else 0 end as a1, case when score >=60 and score <70 then 1 else 0 end as a2, case when score >=70 and score <85 then 1 else 0 end as a3, case when score >=85 and score <=100 then 1 else 0 end as a4 from sc").show()
    sparkSession.sql("select cid, sum(a1),sum(a2),sum(a3),sum(a4) from (select sid, cid, case when score >=0 and score <60 then 1 else 0 end as a1, case when score >=60 and score <70 then 1 else 0 end as a2, case when score >=70 and score <85 then 1 else 0 end as a3, case when score >=85 and score <=100 then 1 else 0 end as a4 from sc) t1 group by cid").show()
    //23 查询各科成绩前三名的记录
    sparkSession.sql("select * from (select *, row_number() over(partition by cid order by score desc) rak from sc ) where rak<=3").show()
    //24 查询每门课程被选修的学生数
    sparkSession.sql("select cid,count(sid) from sc group by cid").show()
    //25 查询出只选修两门课程的学生学号和姓名
    sparkSession.sql("select sid,count(cid) from sc group by sid having count(cid) =2").show()
    //26 查询男生、女生人数
    sparkSession.sql("select sex,count(sid) from student group by sex").show()
    //27 查询名字中含有「风」字的学生信息
    sparkSession.sql("select * from student where sname like '%风%'").show()
    //28 查询同名同性学生名单，并统计同名人数
    sparkSession.sql("select sname,count(sid) from student group by sname having count(sid)>1").show()
    //29 查询 1990 年年出生的学生名单
    sparkSession.sql("select * from student where year(birth)=1990").show()
    //30 查询每门课程的平均成绩，结果按平均成绩降序排列，平均成绩相同时，按课程编号升序排列
    sparkSession.sql("select cid, avg(score) from sc group by cid order by avg(score) desc, cid asc").show()
    //31 查询平均成绩大于等于 85 的所有学生的学号、姓名和平均成绩
    sparkSession.sql("select sid, avg(score) from sc group by sid having avg(score)>=85").show()
    //32 查询课程名称为「数学」，且分数低于 60 的学生姓名和分数
    sparkSession.sql("select sid, score from sc t1 left join course c1 on t1.cid=c1.cid where cname='数学' and score <60").show()
    //33 查询所有学生的课程及分数情况（存在学生没成绩，没选课的情况）
    sparkSession.sql("select * from student s1 left join sc sc1 on s1.sid=sc1.sid ").show()
    //34 查询任何一门课程成绩在 70 分以上的姓名、课程名称和分数
    sparkSession.sql("select * from sc where score >70 ").show()
    //35 查询不及格的课程
    sparkSession.sql("select * from course join sc on course.cid=sc.cid where score <60 ").show()
    //36 查询课程编号为 01 且课程成绩在 80 分以上的学生的学号和姓名
    sparkSession.sql("select * from sc where cid='01' and score>=80").show()
    //37 求每门课程的学生人数
    sparkSession.sql("select cid,count(sid) from sc group by cid").show()
    //38 成绩不重复，查询选修「张三」老师所授课程的学生中，成绩最高的学生信息及其成绩
    sparkSession.sql("select score, sid from (select * from sc sc1 join course c1 on sc1.cid=c1.cid join teacher t1 on t1.tid=c1.tid where t1.tname='张三') t2 order by score desc limit 1").show()
    //39 成绩有重复的情况下，查询选修「张三」老师所授课程的学生中，成绩最高的学生信息及其成绩
    sparkSession.sql("select score, sid from (select * from sc sc1 join course c1 on sc1.cid=c1.cid join teacher t1 on t1.tid=c1.tid where t1.tname='张三') t2 where score in (select max(score) from sc sc1 join course c1 on sc1.cid=c1.cid join teacher t1 on t1.tid=c1.tid where t1.tname='张三')").show()
    sparkSession.sql("select * from (select * from sc sc1 join course c1 on sc1.cid=c1.cid join teacher t1 on t1.tid=c1.tid where t1.tname='张三') t2 where score = (select max(score) from sc sc1 join course c1 on sc1.cid=c1.cid join teacher t1 on t1.tid=c1.tid where t1.tname='张三')").show()
    //40 查询不同课程成绩相同的学生的学生编号、课程编号、学生成绩
    sparkSession.sql("select distinct sc1.* from sc sc1 join sc sc2 on sc1.sid = sc2.sid where sc1.score = sc2.score and sc1.cid!=sc2.cid").show()
    //41 查询每门成绩最好的前两名
    sparkSession.sql("select * from (select cid, sid, row_number() over(partition by cid order by score desc) rak from sc) where rak <=2").show()
    //42 统计每门课程的学生选修人数（超过 5 人的课程才统计）。
    sparkSession.sql("select cid,count(sid) from sc group by cid having count(sid) > 5").show()
    //43 检索至少选修两门课程的学生学号
    sparkSession.sql("select sid,count(cid) from sc group by sid having count(cid) >= 2").show()
    //44 查询选修了全部课程的学生信息
    sparkSession.sql("select sid,count(cid) from sc group by sid having count(cid) = (select count(*) from course)").show()
    //45 查询各学生的年龄，只按年份来算
    sparkSession.sql("select sid, year(now())-year(birth) as age from student").show()
    //46 按照出生日期来算，当前月日 < 出生年月的月日则，年龄减一
    sparkSession.sql("select sid, case when(date_format(now(),'%m-%d')>date_format(birth,'%m-%d')) then year(now())-year(birth) else year(now())-year(birth)+1 end as age from student").show()
    //47 查询本周过生日的学生
    sparkSession.sql("select * from student where weekofyear(now())=weekofyear(birth)").show()
    //48 查询下周过生日的学生
    sparkSession.sql("select * from student where weekofyear(now())+1=weekofyear(birth)").show()
    //49 查询本月过生日的学生
    sparkSession.sql("select * from student where month(now())=month(birth)").show()
    //50 查询下月过生日的学生
    sparkSession.sql("select * from student where (month(now())+4)=month(birth)").show()
    //51 行转列 - pivot函数或者case when ... then ... else end as 两种方式
    sparkSession.sql("select * from sc pivot (sum(score) for sid in ('01','02','03'))").show()
    sparkSession.sql("select * from sc pivot (sum(score) for cid in ('01','02','03','04'))").createOrReplaceTempView("sc_pivot_table")
    //列转行 (spark不支持Unpivot，使用stack来达到同样的效果）
    sparkSession.sql("select sid, stack(4,'01',01,'02',02,'03',03,'04',04) as (cid, score) from sc_pivot_table").show()

    //lateral view explode
    sparkSession.sql("select sid, collect_list(cid) as c_list from sc group by sid").createOrReplaceTempView("sc_list_table")
    sparkSession.sql("select sid, cid from sc_list_table lateral view explode(c_list) tmpTable as cid").show()















  }

}
