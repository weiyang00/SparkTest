package cn.edu360.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by zx on 2017/10/5.
  *
  */
object SubTchTopN1 {

  val topN: Int = 3

  def main(args: Array[String]): Unit = {
    //创建spark配置，设置应用程序名字
    //val conf = new SparkConf().setAppName("ScalaWordCount")
    val conf = new SparkConf().setAppName("ScalaTeacherTopN").setMaster("local[4]")
    //创建spark执行的入口
    val sc = new SparkContext(conf)

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

    // 分组（按学科）
    val grouped: RDD[(String, Iterable[((String, String), Int)])] = reduced.groupBy(_._1._1)

    // 分组后，一个分区内可能有多个学科的数据，一个学科就是一个迭代器
    // 将每一个组拿出来，进行操作
    val sorted = grouped.mapValues(it => it.toList.sortBy(_._2).reverse.take(topN))

    // 收集结果
    val tuples: Array[(String, List[((String, String), Int)])] = sorted.collect()

    println(tuples.toBuffer)

    sc.stop()

  }

}
