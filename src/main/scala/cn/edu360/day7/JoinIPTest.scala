package cn.edu360.day7

import cn.edu360.IpLocation.MyUtils
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

/**
  * Created by zx on 2017/10/14.
  */
object JoinIPTest {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("JoinIPTest")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    // 读取ip 规则
    val ipLines: Dataset[String] = spark.read.textFile("hdfs://120.27.41.246:9000/testDir-weiyang/ips/ip.txt")
    //        val ipLines: Dataset[String] = spark.read.textFile("D://ip.txt")

    // 读取access.log日志
    val logLines: Dataset[String] = spark.read.textFile("hdfs://120.27.41.246:9000/testDir-weiyang/logs/access.log")
    //        val logLines: Dataset[String] = spark.read.textFile("D://access.log")

    // 对数据进行整理,整理ip
    val ipDataset: Dataset[(Long, Long, String)] = ipLines.map(line => {
      val fields = line.split("\\|")
      val startNum = fields(2).toLong
      val endNum = fields(3).toLong
      val city = fields(6)
      (startNum, endNum, city)
    })

    val ipDataFrame: DataFrame = ipDataset.toDF("startNum", "endNum", "city")

    // 对数据进行整理,整理日志
    val logDataset: Dataset[Long] = logLines.map(line => {
      val fields = line.split("\\|")
      val ip = fields(1)
      MyUtils.ip2Long(ip)
    })

    val logDataFrame: DataFrame = logDataset.toDF("ipLong")

    //第一种，创建视图
    ipDataFrame.createTempView("v_ips")
    logDataFrame.createTempView("v_logs")
    val r: DataFrame = spark.sql("SELECT city, COUNT(*) counts FROM v_ips JOIN v_logs ON (ipLong >= startNum AND ipLong <= endNum) GROUP BY city ORDER BY counts DESC ")

    //import org.apache.spark.sql.functions._
    //    val r = ipDataFrame.join(logDataFrame, $"startNum" <= $"ipLong" <= $"endNum", "left_outer")


    r.show()

    spark.stop()

  }
}
