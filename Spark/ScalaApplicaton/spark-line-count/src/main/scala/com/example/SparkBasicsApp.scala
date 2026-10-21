package com.example

import org.apache.spark.sql.{ SparkSession}

object SparkBasicsApp {

  def describeLine(line: String): String =
    s"Matched line: $line (length=${line.length})"

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Scala Spark Playground")
      .master("local[2]")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.shuffle.partitions", "2")
      .getOrCreate()

    val sc = spark.sparkContext
    sc.setLogLevel("WARN")
    try {
        // Test1
        // val rdd = sc.parallelize(Seq(1, 2, 3, 4, 5, 6), numSlices = 3)   // numSlices 指定分区数
        // println("NumPartitions: " + rdd.getNumPartitions)  // 3
        // println("NumDetails: " + rdd.glom().collect().map(_.toList).toList)     // [[1, 2], [3, 4], [5, 6]]，可看到每个分区的内容
    
        // test2
        // val rdd_lines = sc.textFile("data/input.txt")
        // val spark_rdd = rdd_lines.filter(line => line.contains("Spark"))
        // println( "Input had " + spark_rdd.count() + " lines containing 'Spark'. They are:")
        // spark_rdd.foreach(println) // 等价与 spark_rdd.foreach(line => println(line))

        // test3
        // val lines = sc.textFile("data/input.txt")

        // // 匿名函数：告诉 Spark 如何处理每一行，不是在这里立即执行。
        // val trimmedLines = lines.map(line => line.trim)

        // // 函数变量：String => Boolean 表示接收字符串、返回是否保留。
        // // keyword 是闭包捕获的只读值，会随任务发送到 Executor。
        // val keyword = args.headOption.getOrElse("Spark")
        // val containsKeyword: String => Boolean = line => line.contains(keyword)
        // val matchedLines = trimmedLines.filter(containsKeyword)

        // // 对象方法：Scala 根据 map 的参数类型将方法转换成函数。
        // val descriptions = matchedLines.map(SparkBasicsApp.describeLine)

        // // take 触发 Spark 计算；返回数组后的 foreach 在 Driver 上执行。
        // println(s"Up to 10 lines containing '$keyword':")
        // descriptions.take(10).foreach(println)

        // test4
        // val input = sc.parallelize(List(1, 2, 3, 4))
        // val result = input.map(x => x * x)
        // println(result.collect().mkString(","))

        // test5  
        val sentencesRDD = sc.parallelize(Seq("Hello Spark", "Hello RDD"))
        val mappedResult = sentencesRDD.map(sentence => sentence.split(" ")) //RDD[Array[String]]
        val flatMappedResult = sentencesRDD.flatMap(sentence => sentence.split(" "))//RDD[String]
        
        // 输出 map 结果
        mappedResult
          .collect()
          .foreach(array => println(array.mkString("[", ", ", "]")))

        // 输出 flatMap 结果
        flatMappedResult
          .collect()
          .foreach(println)
    } finally {
      spark.stop()
    }
  }
}
