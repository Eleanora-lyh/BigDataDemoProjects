package com.example

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

// 在项目目录运行：sbt "runMain com.example.PairRDDCreationApp"
object PairRDDCreationApp {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Pair RDD Creation")
      .master("local[2]")
      .config("spark.ui.enabled", "false")
      .getOrCreate()

    try {
      val sc = spark.sparkContext
      sc.setLogLevel("WARN")

      // PairRDD 就是元素为二元组 (key, value) 的 RDD，Scala 类型写作 RDD[(K, V)]。
      // 方式一：直接将二元组集合并行化；相同的 key 可以出现多次。
      val pairs: RDD[(String, Int)] = sc.parallelize(
        Seq(("apple", 2), ("banana", 3), ("apple", 1)),
        numSlices = 2
      )
      val pairResult = pairs.collect().sorted.toSeq
      println(s"parallelize tuples: ${pairResult.mkString("[", ", ", "]")}")
      assert(pairResult == Seq(("apple", 1), ("apple", 2), ("banana", 3)))

      // 方式二：用 map 把普通 RDD 的每个元素转换成二元组。
      // word -> 1 与 (word, 1) 等价，常用于词频统计的准备阶段。
      val words = sc.parallelize(Seq("spark", "scala", "spark"), numSlices = 2)
      val wordPairs: RDD[(String, Int)] = words.map(word => (word, 1))
      val wordPairResult = wordPairs.collect().sorted.toSeq
      println(s"map to pairs: ${wordPairResult.mkString("[", ", ", "]")}")
      assert(wordPairResult == Seq(("scala", 1), ("spark", 1), ("spark", 1)))

      // 方式三：keyBy 根据原元素计算 key，并将原元素完整保留为 value。
      val wordsByLength: RDD[(Int, String)] = words.keyBy(word => word.length)
      val keyedResult = wordsByLength.collect().sorted.toSeq
      println(s"keyBy length: ${keyedResult.mkString("[", ", ", "]")}")
      assert(keyedResult == Seq((5, "scala"), (5, "spark"), (5, "spark")))

      // 创建后即可使用 reduceByKey 等键值操作；仅创建 PairRDD 不会自动合并相同 key。
      val wordCounts = wordPairs.reduceByKey(_ + _).collect().sorted.toSeq
      println(s"reduceByKey counts: ${wordCounts.mkString("[", ", ", "]")}")
      assert(wordCounts == Seq(("scala", 1), ("spark", 2)))

      // collect 只适合这里的小数据；排序发生在 Driver 上，方便观察和校验结果。
      println("All PairRDD creation checks passed.")
    } finally {
      spark.stop()
    }
  }
}
