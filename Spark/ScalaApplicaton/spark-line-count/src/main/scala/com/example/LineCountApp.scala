package com.example

import org.apache.spark.sql.SparkSession

object LineCountApp {

  def main(args: Array[String]): Unit = {

    // 如果调用程序时传入了文件路径，就使用第一个参数；
    // 否则默认读取项目下的 data/input.txt。
    val inputPath =
      if (args.nonEmpty) args(0)
      else "data/input.txt"

    // 创建 SparkSession 对象
    val spark = SparkSession
      .builder()
      .appName("Scala Spark Line Count") //设置 Spark 应用名称
      .master("local[*]")// 设置本地运行模式为local，使用所有可用的 CPU 核心，正式项目一般不声明
      .getOrCreate()


    try {
      // 读取输入文件的所有行，返回Dataset[String]
      val lines = spark.read.textFile(inputPath)
      // 触发 Spark Job：计算输入文件的总行数
      val totalLineCount = lines.count()

      // 触发 Spark Job：计算非空行的总行数
      val nonEmptyLineCount = lines
        .filter(line => line.trim.nonEmpty)// 去掉行首和行尾空白。
        .count()

      println(s"Input file: $inputPath")
      println(s"Total line count: $totalLineCount")
      println(s"Non-empty line count: $nonEmptyLineCount")
    } finally {
      spark.stop()
    }
  }
}