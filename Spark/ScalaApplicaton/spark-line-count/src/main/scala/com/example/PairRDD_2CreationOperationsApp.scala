package com.example

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

// 在项目目录运行：sbt "runMain com.example.PairRDD_2CreationOperationsApp"
object PairRDD_2CreationOperationsApp {

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
      // 创建后即可使用 reduceByKey 等键值操作；仅创建 PairRDD 不会自动合并相同 key。
      val wordCounts = wordPairs.reduceByKey((a, b) => a + b)
      val wordCounts2 = words
        .map(word => (word, 1))
        .reduceByKey(_ + _)
      val expectedCounts = Seq(("scala", 1), ("spark", 2))
      assert(wordCounts.collect().sorted.toSeq == expectedCounts)
      val countResult = wordCounts2.collect().sorted.toSeq
      println(s"wordCounts: ${countResult.mkString("[", ", ", "]")}")
      assert(countResult == expectedCounts)

      val pairRDD = words.mapPartitions { iterator =>
        iterator.map(word => (word, 1))
      }
      assert(pairRDD.collect().sorted.toSeq == wordPairResult)

      // 方式三：从文本文件创建 Pair RDD 并按 key 聚合值。
      val linesRDD = sc.textFile("data/name_score.txt")
      val scorePairRDD = linesRDD.map { line =>
        val fields = line.split(",")
        val name = fields(0)
        val score = fields(1).toInt

        (name, score)
      }
      val totalScores = scorePairRDD.reduceByKey(_ + _)
      val scoreResult = totalScores.collect().sorted.toSeq
      println(s"totalScores: ${scoreResult.mkString("[", ", ", "]")}")
      assert(scoreResult == Seq(("alice", 185), ("bob", 85)))

      // 方式四：keyBy 根据原元素计算 key，并将原元素完整保留为 value。
      val wordsByLength: RDD[(Int, String)] = words.keyBy(word => word.length)
      val keyedResult = wordsByLength.collect().sorted.toSeq
      println(s"keyBy length: ${keyedResult.mkString("[", ", ", "]")}")
      assert(keyedResult == Seq((5, "scala"), (5, "spark"), (5, "spark")))

      // collect 只适合这里的小数据；排序发生在 Driver 上，方便观察和校验结果。
      println("All PairRDD creation checks passed.")
    } finally {
      spark.stop()
    }
  }
}
