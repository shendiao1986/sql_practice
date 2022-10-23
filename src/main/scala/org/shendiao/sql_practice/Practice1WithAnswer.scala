package org.shendiao.sql_practice

import org.apache.spark.sql.SparkSession

object Practice1WithAnswer {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[*]").appName("practice").getOrCreate()

    Util.initializeDataViews(spark)

    //1 查询" 01 "课程比" 02 "课程成绩高的学生的信息及课程分数
    /*
    +---+--------+--------+
    |sid|c1_score|c2_score|
    +---+--------+--------+
    |02 |70.0    |60.0    |
    |04 |50.0    |30.0    |
    +---+--------+--------+
     */
    spark.sql("select sc1.sid, sc1.score as c1_score, sc2.score as c2_score from sc sc1 left join sc sc2 on sc1.sid=sc2.sid where sc1.cid='01' and sc2.cid='02' and sc1.score > sc2.score").show(500, false)

    //2 查询同时存在" 01 "课程和" 02 "课程的情况
    /*
    +---+--------+--------+
    |sid|c1_score|c2_score|
    +---+--------+--------+
    |01 |80.0    |90.0    |
    |02 |70.0    |60.0    |
    |03 |80.0    |80.0    |
    |04 |50.0    |30.0    |
    |05 |76.0    |87.0    |
    +---+--------+--------+
     */
    spark.sql("select sc1.sid, sc1.score as c1_score, sc2.score as c2_score from sc sc1 left join sc sc2 on sc1.sid=sc2.sid where sc1.cid='01' and sc2.cid='02'").show(500, false)

    //3 查询存在"01"课程但可能不存在"02"课程的情况(不存在时显示为null)
    /*
    +---+---+-----+----+----+-----+
    |sid|cid|score|sid |cid |score|
    +---+---+-----+----+----+-----+
    |01 |01 |80.0 |01  |02  |90.0 |
    |02 |01 |70.0 |02  |02  |60.0 |
    |03 |01 |80.0 |03  |02  |80.0 |
    |04 |01 |50.0 |04  |02  |30.0 |
    |05 |01 |76.0 |05  |02  |87.0 |
    |06 |01 |31.0 |null|null|null |
    +---+---+-----+----+----+-----+
     */
    spark.sql("select * from (select * from sc where sc.cid='01') as sc1 left join sc sc2 on sc1.sid=sc2.sid and sc2.cid='02'").show(500,false)

    //4 查询不存在"01"课程但存在"02"课程的情况
    /*
    +---+---+-----+----+----+-----+
    |sid|cid|score|sid |cid |score|
    +---+---+-----+----+----+-----+
    |07 |02 |89.0 |null|null|null |
    +---+---+-----+----+----+-----+
     */
    spark.sql("select * from (select * from sc where sc.cid='02') as sc1 left join sc sc2 on sc1.sid=sc2.sid and sc2.cid='01' where sc2.cid is null").show(500,false)

    //5 查询平均成绩大于等于 60 分的同学的学生编号和学生姓名和平均成绩
    /*
    +---+-----+----------+---+---+-----------------+
    |sid|sname|birth     |sex|sid|avg_score        |
    +---+-----+----------+---+---+-----------------+
    |01 |赵雷 |1990-01-01|男 |01 |89.66666666666667|
    |02 |钱电 |1990-12-21|男 |02 |70.0             |
    |03 |孙风 |1990-05-20|男 |03 |80.0             |
    |05 |周梅 |1991-12-01|女 |05 |81.5             |
    |07 |郑竹 |1989-07-01|女 |07 |93.5             |
    +---+-----+----------+---+---+-----------------+
     */
    spark.sql("select * from student s inner join (select sid, avg(score) as avg_score from sc group by sid having avg_score>60) sc1 on s.sid=sc1.sid").show(500,false)

    //6 查询在SC表存在成绩的学生信息
    /*
    +---+-----+----------+---+
    |sid|sname|birth     |sex|
    +---+-----+----------+---+
    |01 |赵雷 |1990-01-01|男 |
    |02 |钱电 |1990-12-21|男 |
    |03 |孙风 |1990-05-20|男 |
    |04 |李云 |1990-08-06|男 |
    |05 |周梅 |1991-12-01|女 |
    |06 |吴兰 |1992-03-01|女 |
    |07 |郑竹 |1989-07-01|女 |
    +---+-----+----------+---+
     */
    spark.sql("select * from student s where s.sid in (select sid from sc)").show(500,false)

    //7 查有成绩的学生信息
    /*
    +---+-----+----------+---+
    |sid|sname|birth     |sex|
    +---+-----+----------+---+
    |01 |赵雷 |1990-01-01|男 |
    |02 |钱电 |1990-12-21|男 |
    |03 |孙风 |1990-05-20|男 |
    |04 |李云 |1990-08-06|男 |
    |05 |周梅 |1991-12-01|女 |
    |06 |吴兰 |1992-03-01|女 |
    |07 |郑竹 |1989-07-01|女 |
    +---+-----+----------+---+
     */
    spark.sql("select * from student s where s.sid in (select sid from sc)").show(500,false)

    //8 查询「李」姓老师的数量
    /*
    +--------+
    |count(1)|
    +--------+
    |1       |
    +--------+
     */
    spark.sql("select count(*) from teacher where tname like '李%'").show(500,false)

    //9 查询学过张三老师授课的同学的信息
    /*
    +---+
    |sid|
    +---+
    |07 |
    |01 |
    |05 |
    |03 |
    |02 |
    |04 |
    +---+
     */
    spark.sql("select distinct sid from sc where cid in (select cid from course c join (select tid from teacher where tname = '张三') t on c.tid=t.tid)").show(500,false)

    //10 查询没有学全所有课程的同学的信息
    /*
    +---+--------+
    |sid|count(1)|
    +---+--------+
    |07 |2       |
    |11 |1       |
    |09 |1       |
    |05 |2       |
    |06 |2       |
    |10 |1       |
    |12 |1       |
    |13 |1       |
    +---+--------+
     */
    spark.sql(" select s.sid, count(*) from student s left join sc sc1 on s.sid = sc1.sid group by s.sid having count(*) != (select count(*) from course)").show(500,false)

    //11 查询至少有一门课与学号为"01"的同学所学相同的同学的信息
    /*
    +---+
    |sid|
    +---+
    |07 |
    |01 |
    |05 |
    |03 |
    |02 |
    |06 |
    |04 |
    +---+
     */
    spark.sql("select distinct sc1.sid from sc sc1 where sc1.sid != '01' and sc1.cid in (select cid from sc where sid='01')").show(500,false)

    //12 查询和"01"号的同学学习的课程完全相同的其他同学的信息
    /*
    +---+---+
    |sid|cnt|
    +---+---+
    |01 |3  |
    |03 |3  |
    |02 |3  |
    |04 |3  |
    +---+---+
     */
    spark.sql("select * from (select sid, count(*) as cnt from sc where cid in (select cid from sc sc1 where sc1.sid='01') group by sid) where cnt = (select count(*) from sc sc1 where sc1.sid='01')").show(500,false)

    //13 查询没学过"张三"老师讲授的任一门课程的学生姓名
    /*
    +---+----------+
    |sid|count(cid)|
    +---+----------+
    |11 |0         |
    |09 |0         |
    |06 |0         |
    |10 |0         |
    |12 |0         |
    |13 |0         |
    +---+----------+
     */
    spark.sql("select sid, count(cid) from (select t1.sid, t2.cid from (select s.sid, sc1.cid from student s left join sc sc1 on s.sid=sc1.sid) t1 left join (select cid from teacher t left join course c on c.tid=t.tid where tname='张三') t2 on t1.cid=t2.cid) group by sid having count(cid) =0").show(500,false)
    spark.sql("select * from student s where s.sid not in(select sid from sc sc1 left join course c on sc1.cid = c.cid left join teacher t on c.tid=t.tid where t.tname='张三')").show()

    //14 查询两门及其以上不及格课程的同学的学号，姓名及其平均成绩
    /*
    +---+------------------+
    |sid|avg(score)        |
    +---+------------------+
    |06 |32.5              |
    |04 |33.333333333333336|
    +---+------------------+
     */
    spark.sql("select sid, avg(score) from sc where sid in (select sid from sc where score <60 group by sid having count(*) >=2) group by sid").show(500,false)

    //15 检索"01"课程分数小于 60，按分数降序排列的学生信息
    /*
    +---+---+-----+
    |sid|cid|score|
    +---+---+-----+
    |04 |01 |50.0 |
    |06 |01 |31.0 |
    +---+---+-----+
     */
    spark.sql("select * from sc where cid='01' and score <60 order by score desc").show(500,false)

    //16 按平均成绩从高到低显示所有学生的所有课程的成绩以及平均成绩
    /*
    +---+---+-----+---+------------------+
    |sid|cid|score|sid|avg_score         |
    +---+---+-----+---+------------------+
    |07 |02 |89.0 |07 |93.5              |
    |07 |03 |98.0 |07 |93.5              |
    |01 |03 |99.0 |01 |89.66666666666667 |
    |01 |01 |80.0 |01 |89.66666666666667 |
    |01 |02 |90.0 |01 |89.66666666666667 |
    |05 |02 |87.0 |05 |81.5              |
    |05 |01 |76.0 |05 |81.5              |
    |03 |02 |80.0 |03 |80.0              |
    |03 |01 |80.0 |03 |80.0              |
    |03 |03 |80.0 |03 |80.0              |
    |02 |03 |80.0 |02 |70.0              |
    |02 |01 |70.0 |02 |70.0              |
    |02 |02 |60.0 |02 |70.0              |
    |04 |02 |30.0 |04 |33.333333333333336|
    |04 |01 |50.0 |04 |33.333333333333336|
    |04 |03 |20.0 |04 |33.333333333333336|
    |06 |01 |31.0 |06 |32.5              |
    |06 |03 |34.0 |06 |32.5              |
    +---+---+-----+---+------------------+
     */
    spark.sql("select * from sc sc1 left join (select sid, avg(score) avg_score from sc group by sid) sc2 on sc1.sid=sc2.sid order by avg_score desc").show(500,false)

    //17 查询各科成绩最高分、最低分和平均分
    /*
    +---+----------+----------+-----------------+
    |cid|max(score)|min(score)|avg(score)       |
    +---+----------+----------+-----------------+
    |01 |80.0      |31.0      |64.5             |
    |03 |99.0      |20.0      |68.5             |
    |02 |90.0      |30.0      |72.66666666666667|
    +---+----------+----------+-----------------+
     */
    spark.sql("select cid, max(score), min(score), avg(score) from sc group by cid").show(500,false)

    //18 按各科成绩进行排序，并显示排名， Score 重复时保留名次空缺
    /*
    +---+---+-----+---+
    |sid|cid|score|rak|
    +---+---+-----+---+
    |01 |01 |80.0 |1  |
    |03 |01 |80.0 |1  |
    |05 |01 |76.0 |3  |
    |02 |01 |70.0 |4  |
    |04 |01 |50.0 |5  |
    |06 |01 |31.0 |6  |
    |01 |03 |99.0 |1  |
    |07 |03 |98.0 |2  |
    |02 |03 |80.0 |3  |
    |03 |03 |80.0 |3  |
    |06 |03 |34.0 |5  |
    |04 |03 |20.0 |6  |
    |01 |02 |90.0 |1  |
    |07 |02 |89.0 |2  |
    |05 |02 |87.0 |3  |
    |03 |02 |80.0 |4  |
    |02 |02 |60.0 |5  |
    |04 |02 |30.0 |6  |
    +---+---+-----+---+
     */
    spark.sql("select sid, cid, score, rank() over (partition by cid order by score desc) as rak from sc").show(500,false)

    //19 按各科成绩进行行排序，并显示排名， Score 重复时合并名次
    /*
    +---+---+-----+---+
    |sid|cid|score|rak|
    +---+---+-----+---+
    |01 |01 |80.0 |1  |
    |03 |01 |80.0 |1  |
    |05 |01 |76.0 |2  |
    |02 |01 |70.0 |3  |
    |04 |01 |50.0 |4  |
    |06 |01 |31.0 |5  |
    |01 |03 |99.0 |1  |
    |07 |03 |98.0 |2  |
    |02 |03 |80.0 |3  |
    |03 |03 |80.0 |3  |
    |06 |03 |34.0 |4  |
    |04 |03 |20.0 |5  |
    |01 |02 |90.0 |1  |
    |07 |02 |89.0 |2  |
    |05 |02 |87.0 |3  |
    |03 |02 |80.0 |4  |
    |02 |02 |60.0 |5  |
    |04 |02 |30.0 |6  |
    +---+---+-----+---+
     */
    spark.sql("select sid, cid, score, dense_rank() over (partition by cid order by score desc) as rak from sc").show(500,false)

    //20 查询学生的总成绩，并进行排名，总分重复时保留名次空缺
    /*
    +---+-----------+---+
    |sid|total_score|rak|
    +---+-----------+---+
    |06 |65.0       |1  |
    |04 |100.0      |2  |
    |05 |163.0      |3  |
    |07 |187.0      |4  |
    |02 |210.0      |5  |
    |03 |240.0      |6  |
    |01 |269.0      |7  |
    +---+-----------+---+
     */
    spark.sql("select sid, total_score, rank() over (order by total_score) rak from (select sid, sum(score) as total_score from sc group by sid)").show(500,false)
    //21 查询学生的总成绩，并进行排名，总分重复时保留合并名次空缺
    /*
    +---+-----------+---+
    |sid|total_score|rak|
    +---+-----------+---+
    |06 |65.0       |1  |
    |04 |100.0      |2  |
    |05 |163.0      |3  |
    |07 |187.0      |4  |
    |02 |210.0      |5  |
    |03 |240.0      |6  |
    |01 |269.0      |7  |
    +---+-----------+---+
     */
    spark.sql("select sid, total_score, dense_rank() over (order by total_score) rak from (select sid, sum(score) as total_score from sc group by sid)").show(500,false)
    //22 统计各科成绩各分数段人数：课程编号，课程名称，[100-85]，[85-70]，[70-60]，[60-0] 及所占百分比
    /*
    +---+------+------+-------+------+-------+------+--------+------+
    |cid|[60-0]|R1    |[70-60]|R2    |[85-70]|R3    |[100-85]|R4    |
    +---+------+------+-------+------+-------+------+--------+------+
    |01 |2     |33.33%|0      |0.0%  |4      |66.67%|0       |0.0%  |
    |03 |2     |33.33%|0      |0.0%  |2      |33.33%|2       |33.33%|
    |02 |1     |16.67%|1      |16.67%|1      |16.67%|3       |50.0% |
    +---+------+------+-------+------+-------+------+--------+------+
     */
    spark.sql("select cid, sum(case when score < 60 then 1 else 0 end) as `[60-0]`, CONCAT(ROUND(sum(case when score < 60 then 1 else 0 end)*100/count(*),2),'%') as R1,sum(case when score >= 60 and score <70 then 1 else 0 end) as `[70-60]`, CONCAT(ROUND(sum(case when score >= 60 and score <70 then 1 else 0 end)*100/count(*),2),'%') as R2, sum(case when score >= 70 and score <85 then 1 else 0 end) as `[85-70]`, CONCAT(ROUND(sum(case when score >= 70 and score <85 then 1 else 0 end)*100/count(*),2),'%') as R3, sum(case when score >= 85 and score <=100 then 1 else 0 end) as `[100-85]`, CONCAT(ROUND(sum(case when score >= 85 and score <=100 then 1 else 0 end)*100/count(*),2),'%') as R4 from sc group by cid").show(500,false)
    //23 查询各科成绩前三名的记录
    /*
    +---+---+-----+---+
    |cid|sid|score|rak|
    +---+---+-----+---+
    |01 |01 |80.0 |1  |
    |01 |03 |80.0 |1  |
    |01 |05 |76.0 |3  |
    |03 |01 |99.0 |1  |
    |03 |07 |98.0 |2  |
    |03 |02 |80.0 |3  |
    |03 |03 |80.0 |3  |
    |02 |01 |90.0 |1  |
    |02 |07 |89.0 |2  |
    |02 |05 |87.0 |3  |
    +---+---+-----+---+
     */
    spark.sql("select * from (select cid, sid, score, rank() over (partition by cid order by score desc) rak from sc) where rak <=3").show(500,false)
    //24 查询每门课程被选修的学生数
    /*
    +---+----------+
    |cid|count(sid)|
    +---+----------+
    | 01|         6|
    | 03|         6|
    | 02|         6|
    +---+----------+
     */
    spark.sql("").show(500,false)
    //25 查询出只选修两门课程的学生学号和姓名
    /*
    +---+----------+
    |sid|count(cid)|
    +---+----------+
    | 07|         2|
    | 05|         2|
    | 06|         2|
    +---+----------+
     */
    spark.sql("select sid, count(cid) from sc group by sid having count(cid)==2").show(500,false)
    //26 查询男生、女生人数
    /*
    +---+--------+
    |sex|count(1)|
    +---+--------+
    | 男|       4|
    | 女|       8|
    +---+--------+
     */
    spark.sql("select sex, count(*) from student group by sex").show(500,false)
    //27 查询名字中含有「风」字的学生信息
    spark.sql("select * from student where sname like '%风%'").show(500,false)
    //28 查询同名同性学生名单，并统计同名人数
    /*
    +-----+--------+
    |sname|count(1)|
    +-----+--------+
    | 李四|       2|
    +-----+--------+
     */
    spark.sql("select sname, count(*) from student group by sname having count(*) >1").show(500,false)
    //29 查询 1990 年年出生的学生名单
    /*
    +---+-----+----------+---+
    |sid|sname|     birth|sex|
    +---+-----+----------+---+
    | 01| 赵雷|1990-01-01| 男|
    | 02| 钱电|1990-12-21| 男|
    | 03| 孙风|1990-05-20| 男|
    | 04| 李云|1990-08-06| 男|
    +---+-----+----------+---+
     */
    spark.sql("select * from student where year(birth)=1990").show(500,false)
    //30 查询每门课程的平均成绩，结果按平均成绩降序排列，平均成绩相同时，按课程编号升序排列
    /*
    +---+-----------------+
    |cid|        avg_score|
    +---+-----------------+
    | 02|72.66666666666667|
    | 03|             68.5|
    | 01|             64.5|
    +---+-----------------+
     */
    spark.sql("select cid, avg(score) as avg_score from sc group by cid order by avg(score) desc, cid asc").show(500,false)
    //31 查询平均成绩大于等于 85 的所有学生的学号、姓名和平均成绩
    /*
    +---+-----+-----------------+
    |sid|sname|        avg_score|
    +---+-----+-----------------+
    | 07| 郑竹|             93.5|
    | 01| 赵雷|89.66666666666667|
    +---+-----+-----------------+
     */
    spark.sql("select sc1.sid, sname, avg_score from (select sid, avg(score) as avg_score from sc group by sid having avg(score) >= 85) sc1 left join student s1 on sc1.sid=s1.sid").show(500,false)
    //32 查询课程名称为「数学」，且分数低于 60 的学生姓名和分数
    spark.sql("select sid, score from sc t1 left join course c1 on t1.cid=c1.cid where cname='数学' and score <60").show(500,false)
    //33 查询所有学生的课程及分数情况（存在学生没成绩，没选课的情况）
    /*
    +---+-----+----------+---+----+----+-----+
    |sid|sname|     birth|sex| sid| cid|score|
    +---+-----+----------+---+----+----+-----+
    | 01| 赵雷|1990-01-01| 男|  01|  03| 99.0|
    | 01| 赵雷|1990-01-01| 男|  01|  02| 90.0|
    | 01| 赵雷|1990-01-01| 男|  01|  01| 80.0|
    | 02| 钱电|1990-12-21| 男|  02|  03| 80.0|
    | 02| 钱电|1990-12-21| 男|  02|  02| 60.0|
    | 02| 钱电|1990-12-21| 男|  02|  01| 70.0|
    | 03| 孙风|1990-05-20| 男|  03|  03| 80.0|
    | 03| 孙风|1990-05-20| 男|  03|  02| 80.0|
    | 03| 孙风|1990-05-20| 男|  03|  01| 80.0|
    | 04| 李云|1990-08-06| 男|  04|  03| 20.0|
    | 04| 李云|1990-08-06| 男|  04|  02| 30.0|
    | 04| 李云|1990-08-06| 男|  04|  01| 50.0|
    | 05| 周梅|1991-12-01| 女|  05|  02| 87.0|
    | 05| 周梅|1991-12-01| 女|  05|  01| 76.0|
    | 06| 吴兰|1992-03-01| 女|  06|  03| 34.0|
    | 06| 吴兰|1992-03-01| 女|  06|  01| 31.0|
    | 07| 郑竹|1989-07-01| 女|  07|  03| 98.0|
    | 07| 郑竹|1989-07-01| 女|  07|  02| 89.0|
    | 09| 张三|2017-12-20| 女|null|null| null|
    | 10| 李四|2017-12-25| 女|null|null| null|
    +---+-----+----------+---+----+----+-----+
     */
    spark.sql("select * from student left join sc on student.sid=sc.sid").show(500,false)
    //34 查询任何一门课程成绩在 70 分以上的姓名、课程名称和分数
    /*
    +-----+---+-----+
    |sname|cid|score|
    +-----+---+-----+
    | 赵雷| 01| 80.0|
    | 赵雷| 02| 90.0|
    | 赵雷| 03| 99.0|
    | 钱电| 03| 80.0|
    | 孙风| 01| 80.0|
    | 孙风| 02| 80.0|
    | 孙风| 03| 80.0|
    | 周梅| 01| 76.0|
    | 周梅| 02| 87.0|
    | 郑竹| 02| 89.0|
    | 郑竹| 03| 98.0|
    +-----+---+-----+
     */
    spark.sql("select sname, cid, score from sc left join student on sc.sid=student.sid where score > 70").show(500,false)
    //35 查询不及格的课程
    /*
    +---+---+-----+
    |sid|cid|score|
    +---+---+-----+
    | 04| 01| 50.0|
    | 04| 02| 30.0|
    | 04| 03| 20.0|
    | 06| 01| 31.0|
    | 06| 03| 34.0|
    +---+---+-----+
     */
    spark.sql("select * from sc where score < 60").show(500,false)
    //36 查询课程编号为 01 且课程成绩在 80 分以上的学生的学号和姓名
    /*
    +---+-----+-----+
    |sid|sname|score|
    +---+-----+-----+
    | 01| 赵雷| 80.0|
    | 03| 孙风| 80.0|
    +---+-----+-----+
     */
    spark.sql("select sc.sid, student.sname, sc.score from sc left join student on sc.sid=student.sid where cid='01' and score >= 80").show(500,false)
    //37 求每门课程的学生人数
    /*
    +---+----------+
    |cid|count(sid)|
    +---+----------+
    | 01|         6|
    | 03|         6|
    | 02|         6|
    +---+----------+
     */
    spark.sql("select cid, count(sid) from sc group by cid").show(500,false)
    //38 成绩不重复，查询选修「张三」老师所授课程的学生中，成绩最高的学生信息及其成绩
    spark.sql("select score, sid from (select * from sc sc1 join course c1 on sc1.cid=c1.cid join teacher t1 on t1.tid=c1.tid where t1.tname='张三') t2 order by score desc limit 1").show(500,false)
    //39 成绩有重复的情况下，查询选修「张三」老师所授课程的学生中，成绩最高的学生信息及其成绩
    spark.sql("select * from (select * from sc sc1 join course c1 on sc1.cid=c1.cid join teacher t1 on t1.tid=c1.tid where t1.tname='张三') t2 where score = (select max(score) from sc sc1 join course c1 on sc1.cid=c1.cid join teacher t1 on t1.tid=c1.tid where t1.tname='张三')").show(500,false)
    //40 查询不同课程成绩相同的学生的学生编号、课程编号、学生成绩
    /*
    +---+---+-----+
    |sid|cid|score|
    +---+---+-----+
    | 01| 01| 80.0|
    | 02| 03| 80.0|
    | 03| 01| 80.0|
    | 03| 02| 80.0|
    | 03| 03| 80.0|
    +---+---+-----+
     */
    spark.sql("select * from sc where score in (select score from sc group by score having count(*) >=2)").show(500,false)
    //41 查询每门成绩最好的前两名
    /*
    +---+---+-----+---+
    |sid|cid|score|rak|
    +---+---+-----+---+
    | 01| 01| 80.0|  1|
    | 03| 01| 80.0|  1|
    | 01| 03| 99.0|  1|
    | 07| 03| 98.0|  2|
    | 01| 02| 90.0|  1|
    | 07| 02| 89.0|  2|
    +---+---+-----+---+
     */
    spark.sql("select * from (select sc.*, rank() over(partition by cid order by score desc) as rak from sc) where rak<=2").show(500,false)
    //42 统计每门课程的学生选修人数（超过 5 人的课程才统计）。
    /*
    +---+
    |cid|
    +---+
    | 01|
    | 03|
    | 02|
    +---+
     */
    spark.sql("select cid from (select sc.*, row_number() over(partition by cid order by score desc) as row_num from sc) group by cid having max(row_num)>=5").show(500,false)
    //43 检索至少选修三门课程的学生学号
    /*
    +---+
    |sid|
    +---+
    | 01|
    | 03|
    | 02|
    | 04|
    +---+
     */
    spark.sql("select sid from sc group by sid having count(*) >=3").show(500,false)
    //44 查询选修了全部课程的学生信息
    /*
    +---+
    |sid|
    +---+
    | 01|
    | 03|
    | 02|
    | 04|
    +---+
     */
    spark.sql("select sid from sc group by sid having count(*) = (select count(*) from course)").show(500,false)
    //45 查询各学生的年龄，只按年份来算
    /*
    +---------------------------------------------------------------------+
    |(year(CAST(current_timestamp() AS DATE)) - year(CAST(birth AS DATE)))|
    +---------------------------------------------------------------------+
    |                                                                   32|
    |                                                                   32|
    |                                                                   32|
    |                                                                   32|
    |                                                                   31|
    |                                                                   30|
    |                                                                   33|
    |                                                                    5|
    |                                                                    5|
    |                                                                    5|
    |                                                                    5|
    |                                                                    4|
    +---------------------------------------------------------------------+
     */
    spark.sql("select year(now())-year(birth) from student").show(500,false)
    //46 按照出生日期来算，当前月日 < 出生年月的月日则，年龄减一
    spark.sql("select sid, case when(date_format(now(),'%m-%d')>date_format(birth,'%m-%d')) then year(now())-year(birth) else year(now())-year(birth)+1 end as age from student").show(500,false)
    //47 查询本周过生日的学生
    spark.sql("select * from student where weekofyear(now())=weekofyear(birth)").show(500,false)
    //48 查询下周过生日的学生
    spark.sql("select * from student where weekofyear(now())+1=weekofyear(birth)").show(500,false)
    //49 查询本月过生日的学生
    spark.sql("select * from student where month(now())=month(birth)").show(500,false)
    //50 查询下月过生日的学生
    spark.sql("select * from student where month(now())+1=month(birth)").show(500,false)
    //51 行转列 - pivot函数或者case when ... then ... else end as 两种方式
    //51.1 将性别sex列的值转化为对应的标签，以列的形式显示出来，使用case when实现
    /*
    +---+-----+-------+---------+
    |sid|sname|is_male|is_female|
    +---+-----+-------+---------+
    |01 |赵雷 |1      |0        |
    |02 |钱电 |1      |0        |
    |03 |孙风 |1      |0        |
    |04 |李云 |1      |0        |
    |05 |周梅 |0      |1        |
    |06 |吴兰 |0      |1        |
    |07 |郑竹 |0      |1        |
    |09 |张三 |0      |1        |
    |10 |李四 |0      |1        |
    |11 |李四 |0      |1        |
    |12 |赵六 |0      |1        |
    |13 |孙七 |0      |1        |
    +---+-----+-------+---------+
     */
    spark.sql("select sid, sname, case when sex ='male' then 1 else 0 end as is_male,case when sex ='female' then 1 else 0 end as is_female from student").show(500,false)
    //51.2 将性别sex列的值转化为对应的标签，以列的形式显示出来，使用pivot实现
    /*
    +---+-----+-------+---------+
    |sid|sname|is_male|is_female|
    +---+-----+-------+---------+
    |01 |赵雷 |1      |null     |
    |02 |钱电 |1      |null     |
    |03 |孙风 |1      |null     |
    |04 |李云 |1      |null     |
    |05 |周梅 |null   |1        |
    |06 |吴兰 |null   |1        |
    |07 |郑竹 |null   |1        |
    |09 |张三 |null   |1        |
    |10 |李四 |null   |1        |
    |11 |李四 |null   |1        |
    |12 |赵六 |null   |1        |
    |13 |孙七 |null   |1        |
    +---+-----+-------+---------+
     */
    spark.sql("select sid, sname, is_male, is_female from student pivot(count(sex) for sex in ('male' as is_male,'female' as is_female)) order by sid").show(500,false)
    //列转行 (spark不支持Unpivot，使用stack来达到同样的效果），将51.1的转换为sid,sex的形式
    /*
    +---+----+
    |sid|sex |
    +---+----+
    |01 |male|
    |02 |male|
    +---+----+
     */
    spark.sql("select sid, case when sum(s_sex) = 1 then 'male' else 'female' end as sex from (select sid, stack(2, '01', is_male,'02', is_male) as (s_sid, s_sex) from (select sid, sname, case when sex ='male' then 1 else 0 end as is_male,case when sex ='female' then 1 else 0 end as is_female from student)) where sid=s_sid group by sid").show(500,false)
    //53 统计到过西安的学生的个数(lateral view 列转行)
    /*
    +-------------------+
    |count(DISTINCT sid)|
    +-------------------+
    |3                  |
    +-------------------+
     */
    spark.sql("select count(distinct(sid)) from student lateral view explode(split(visited,',')) as visit_place where visit_place='xi an'").show(500,false)

  }



}
