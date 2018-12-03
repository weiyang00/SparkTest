package cn.edu360.wrodcount

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 每个学科最受欢迎的老师
  * 埋点 checkpoint
  * 过滤多次提交
  * cache缓存到内存中
  */

object SubTchTopN2 {

  val N: Int = 3

  def main(args: Array[String]): Unit = {

    //创建spark配置，设置应用程序名字
    //val conf = new SparkConf().setAppName("ScalaWordCount")
    val conf = new SparkConf().setAppName("ScalaTeacherTopN").setMaster("local[4]")
    //创建spark执行的入口
    val sc = new SparkContext(conf)

    // 设置checkpoint（埋点）的位置
    //    sc.setCheckpointDir("hdfs://host-01:9000/checkpoint")

    // 在driver端获取到全部的规则数据

    sc.broadcast()

    //指定以后从哪里读取数据创建RDD（弹性分布式数据集）
    val lines: RDD[String] = sc.textFile(args(0))
    // 切分压平
    val subAndTeachersAndOne: RDD[((String, String), Int)] = lines.map(x => {
      val strs = x.split("/")
      val subPath: String = strs(2)
      val sub = subPath.split("\\.")(0)
      val tea: String = strs(3)
      ((sub, tea), 1)
    })
    // 聚合， 将学科sub 和老师tea 联合当做key
    val reduced: RDD[((String, String), Int)] = subAndTeachersAndOne.reduceByKey(_ + _)

    // 计算共有哪些学科
    val subs: Array[String] = reduced.map(_._1._1).distinct().collect()

    // 缓存cache到内存，提高运算速度
    val cached = reduced.cache()

    // 埋点
    //    val checkpoint = reduced.checkpoint

    for (elem <- subs) {
      // 过滤后只剩一个学科的数据
      val filtered: RDD[((String, String), Int)] = cached.filter(_._1._1 == elem)

      // RDD 排序去前topN
      val sorted = filtered.sortBy(_._2, false).take(N)

      println(sorted.toBuffer)
    }

    // 前面的cached的数据已经计算完了，后面还有很多其他的指标要计算
    // 后面计算的指标也要触发多次action， 最好将数据缓存到内存
    // 原来的数据占用了内存，必须释放
    cached.unpersist(true) // true == 异步 ； false == 同步

    sc.stop()

  }

}
