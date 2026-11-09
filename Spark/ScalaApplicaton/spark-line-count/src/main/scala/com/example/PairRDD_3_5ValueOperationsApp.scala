package com.example

import org.apache.spark.sql.SparkSession
import org.apache.spark.HashPartitioner

// 在项目目录运行：sbt "runMain com.example.PairRDD_3_5ValueOperationsApp"
object PairRDD_3_5ValueOperationsApp {

  private def checkResult[T](label: String, actual: T, expected: T): Unit = {
    println(s"$label: $actual")
    assert(actual == expected, s"$label: expected $expected, got $actual")
  }

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Pair RDD Value Operations")
      .master("local[2]")
      .config("spark.ui.enabled", "false")
      .getOrCreate()

    try {
      val sc = spark.sparkContext
      sc.setLogLevel("WARN")
      val rdd = sc.parallelize(Seq((1, 2), (3, 4), (3, 6)), numSlices = 2)

      // map/filter 接收整个二元组；mapValues 只接收 Value，不改变 Key。
      val mapped = rdd.map { case (key, value) => (key + 1, value + 1) }
      checkResult("map: transform whole pair", mapped.collect().sorted.toSeq, Seq((2, 3), (4, 5), (4, 7)))
      val filtered = rdd.filter { case (_, value) => value > 2 }
      checkResult("filter: values greater than 2", filtered.collect().sorted.toSeq, Seq((3, 4), (3, 6)))

      val mappedValues = rdd.mapValues(x => x + 1)
      checkResult("mapValues", mappedValues.collect().sorted.toSeq, Seq((1, 3), (3, 5), (3, 7)))

      // 一个 Value 可以展开为零条或多条记录，每条记录保留原 Key。
      // 6 to 5 是空 Range，因此 (3, 6) 不产生输出；不是把所有 Value 合成一个集合。
      val expandedValues = rdd.flatMapValues(x => x to 5)
      checkResult(
        "flatMapValues",
        expandedValues.collect().sorted.toSeq,
        Seq((1, 2), (1, 3), (1, 4), (1, 5), (3, 4), (3, 5))
      )

      checkResult("keys: duplicates retained", rdd.keys.collect().sorted.toSeq, Seq(1, 3, 3))
      checkResult("values", rdd.values.collect().sorted.toSeq, Seq(2, 4, 6))
      checkResult("distinct keys", rdd.keys.distinct().collect().sorted.toSeq, Seq(1, 3))

      // Value 可以从数字变为集合，再展开为字符串；输出类型无需与输入一致。
      val words = sc.parallelize(
        Seq(("fruit", "apple banana"), ("drink", "tea")),
        numSlices = 2
      )
      val tokens = words.flatMapValues(_.split(" "))
      checkResult(
        "flatMapValues: tokenize text",
        tokens.collect().sorted.toSeq,
        Seq(("drink", "tea"), ("fruit", "apple"), ("fruit", "banana"))
      )

      val tokensByFlatMap = words.flatMap { case (key, text) =>
        text.split(" ").map(word => (key, word))
      }
      val tokensByFlatMapValues = words.flatMapValues(_.split(" "))
      checkResult(
        "flatMap vs flatMapValues: same records",
        tokensByFlatMap.collect().sorted.toSeq,
        tokensByFlatMapValues.collect().sorted.toSeq
      )
      val allWords = words.flatMap { case (_, text) => text.split(" ") }
      checkResult("flatMap: ordinary RDD", allWords.collect().sorted.toSeq, Seq("apple", "banana", "tea"))
      val categoriesByWord = words.flatMap { case (category, text) =>
        text.split(" ").map(word => (word, category))
      }
      checkResult(
        "flatMap: choose a new key",
        categoriesByWord.collect().sorted.toSeq,
        Seq(("apple", "fruit"), ("banana", "fruit"), ("tea", "drink"))
      )

      // 必须先设置分区器，才能观察两种展开操作的分区信息差异。
      val partitioner = new HashPartitioner(2)
      val partitionedWords = words.partitionBy(partitioner)
      val expandedPairs = partitionedWords.flatMap { case (key, text) =>
        text.split(" ").map(word => (key, word))
      }
      checkResult("flatMap: discards partitioner", expandedPairs.partitioner, None)
      checkResult(
        "flatMapValues: preserves partitioner",
        partitionedWords.flatMapValues(_.split(" ")).partitioner,
        Some(partitioner)
      )

      println("All PairRDD value operation checks passed.")
    } finally {
      spark.stop()
    }
  }
}
