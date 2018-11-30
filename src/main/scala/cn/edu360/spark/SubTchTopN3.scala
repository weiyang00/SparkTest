package cn.edu360.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{Partitioner, SparkConf, SparkContext}

import scala.collection.mutable

/**
  * Created by zx on 2017/10/5.
  *
  */

// 分区器
object SubTchTopN3 {

  val N: Int = 3

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

    // 计算共有哪些学科
    val subs: Array[String] = subAndTeachersAndOne.map(_._1._1).distinct().collect()

    // 自定义的分区器
    val partitioner: Partitioner = new SubPartitioner(subs)

    // 聚合， 将学科sub 和老师tea 联合当做key
    val reduced: RDD[((String, String), Int)] = subAndTeachersAndOne.reduceByKey(partitioner, _ + _)

    //    // 自定义一个分区器，并按指定的分区器进行分区
    //    val partitioned: RDD[((String, String), Int)] = reduced.partitionBy(partitioner)

    // 如果一次拿出一个分区，就可以操作一个分区中的数据了
    // 将迭代器 iterator 转成 List， 排序， 再转成 iterator
    val sorted: RDD[((String, String), Int)] = reduced.mapPartitions(it => {
      //
      //      var treeSet =  mutable.TreeSet(N+1)
      //
      //      while (it.hasNext){
      //        var count: Int = it.next()._2
      //
      //      }

      it.toList.sortBy(_._2).reverse.take(N).iterator
      //即排序又不全部加载的内存

      //长度为N+1的一个可排序集合 ThreeSet, 只比较最小的

    })

    //
    val r: Array[((String, String), Int)] = sorted.collect()

    println(r.toBuffer)

    sc.stop()

  }

}

class SubPartitioner(sbs: Array[String]) extends Partitioner {

  // 用于存放映射的map
  val rules = new mutable.HashMap[String, Int]()

  var i = 0
  for (elem <- sbs) {
    rules.put(elem, i)
    i += 1
  }

  // 返回分区的数量 -- 即下一个RDD的分区数量
  override def numPartitions = sbs.length

  // 根据传入的key计算分区编号
  // key 是一个元祖（String， String）
  override def getPartition(key: Any): Int = {
    // 学科
    val sub = key.asInstanceOf[(String, String)]._1

    rules(sub)
  }

}
