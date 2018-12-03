package cn.edu360.wrodcount

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by zx on 2017/10/5.
  *
  */
object ScalaTeacherTopN {

  def main(args: Array[String]): Unit = {
    //创建spark配置，设置应用程序名字
    //val conf = new SparkConf().setAppName("ScalaWordCount")
    val conf = new SparkConf().setAppName("ScalaTeacherTopN").setMaster("local[4]")
    //创建spark执行的入口
    val sc = new SparkContext(conf)

    val lines: RDD[String] = sc.textFile(args(0))
    val teachers: RDD[String] = lines.map(_.split("/")(3))
    val teaAndOne: RDD[(String, Int)] = teachers.map((_, 1))
    val reduced: RDD[(String, Int)] = teaAndOne.reduceByKey(_ + _)
    val sorted: RDD[(String, Int)] = reduced.sortBy(_._2, false)
    sorted.saveAsTextFile("D://teacherTopN")

    sc.stop()

  }

}
