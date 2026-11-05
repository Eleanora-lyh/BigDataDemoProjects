package com.example

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

// 在项目目录运行：sbt "runMain com.example.PairRDD_3_2GroupingOperationsApp"
object PairRDD_3_2GroupingOperationsApp {

  private def checkResult[T](label: String, actual: T, expected: T): Unit = {
    println(s"$label: $actual")
    assert(actual == expected, s"$label: expected $expected, got $actual")
  }

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Pair RDD Grouping")
      .master("local[2]")
      .config("spark.ui.enabled", "false")
      .getOrCreate()

    try {
      val sc = spark.sparkContext
      sc.setLogLevel("WARN")

      val scores: RDD[(String, Int)] = sc.parallelize(
        Seq(("Alice", 80), ("Bob", 70), ("Alice", 90), ("Bob", 70), ("Cindy", 100)),
        numSlices = 2
      )
      println("scores = [(Alice,80), (Bob,70), (Alice,90), (Bob,70), (Cindy,100)]")

      // 1. groupByKey：按已有 key 分组，只收集 value，不求和，也不去重。
      // RDD[(K, V)] -> RDD[(K, Iterable[V])]，Bob 的两个 70 都会保留。
      // 分组内的顺序没有保证；此处排序仅为稳定展示和校验，不代表原始顺序。
      val grouped: RDD[(String, Iterable[Int])] = scores.groupByKey()
      val expectedGroups = Seq(
        ("Alice", List(80, 90)),
        ("Bob", List(70, 70)),
        ("Cindy", List(100))
      )
      checkResult(
        "groupByKey: scores by student",
        grouped.mapValues(_.toList.sorted).collect().sortBy(_._1).toSeq,
        expectedGroups
      )

      // 2. mapValues：在分组结果上处理每组数据，例如取最高分、记录数。
      val summaries = grouped.mapValues(values => (values.max, values.size))
      checkResult(
        "groupByKey + mapValues: (max, count)",
        summaries.collect().sortBy(_._1).toSeq,
        Seq(("Alice", (90, 2)), ("Bob", (70, 2)), ("Cindy", (100, 1)))
      )

      // 仅求和时优先 reduceByKey：可先在分区内合并，通常比传输全部 value 更省资源。
      // groupByKey 适合确实需要组内明细的场景；大 key 仍可能造成内存压力和数据倾斜。
      val expectedTotals = Seq(("Alice", 170), ("Bob", 140), ("Cindy", 100))
      checkResult(
        "groupByKey + sum: totals",
        grouped.mapValues(_.sum).collect().sortBy(_._1).toSeq,
        expectedTotals
      )
      checkResult(
        "reduceByKey: totals",
        scores.reduceByKey(_ + _).collect().sortBy(_._1).toSeq,
        expectedTotals
      )

      // 3. cogroup：将多个 Pair RDD 按同一个 key 对齐，每个来源保留独立的集合。
      // 两个 RDD 的 value 类型可以不同，结果包含双方 key 的并集；缺失一侧为空集合。
      val clubs: RDD[(String, String)] = sc.parallelize(
        Seq(("Alice", "Chess"), ("Alice", "Music"), ("Bob", "Sports"), ("David", "Art")),
        numSlices = 2
      )
      val cogrouped: RDD[(String, (Iterable[Int], Iterable[String]))] =
        scores.cogroup(clubs)
      checkResult(
        "cogroup: (scores, clubs)",
        cogrouped.mapValues { case (studentScores, studentClubs) =>
          (studentScores.toList.sorted, studentClubs.toList.sorted)
        }.collect().sortBy(_._1).toSeq,
        Seq(
          ("Alice", (List(80, 90), List("Chess", "Music"))),
          ("Bob", (List(70, 70), List("Sports"))),
          ("Cindy", (List(100), List.empty[String])),
          ("David", (List.empty[Int], List("Art")))
        )
      )

      // 4. 三个 RDD 的 cogroup：每个 key 对应三个集合，而不是嵌套的二元组。
      val cities: RDD[(String, String)] = sc.parallelize(
        Seq(("Alice", "Beijing"), ("Cindy", "Shanghai"), ("Eva", "Shenzhen")),
        numSlices = 2
      )
      val threeWay: RDD[(String, (Iterable[Int], Iterable[String], Iterable[String]))] =
        scores.cogroup(clubs, cities)
      checkResult(
        "cogroup with three RDDs: (scores, clubs, cities)",
        threeWay.mapValues { case (studentScores, studentClubs, studentCities) =>
          (studentScores.toList.sorted, studentClubs.toList.sorted, studentCities.toList.sorted)
        }.collect().sortBy(_._1).toSeq,
        Seq(
          ("Alice", (List(80, 90), List("Chess", "Music"), List("Beijing"))),
          ("Bob", (List(70, 70), List("Sports"), List.empty[String])),
          ("Cindy", (List(100), List.empty[String], List("Shanghai"))),
          ("David", (List.empty[Int], List("Art"), List.empty[String])),
          ("Eva", (List.empty[Int], List.empty[String], List("Shenzhen")))
        )
      )

      // 5. join 对比：只保留双方都有的 key，对组内左右 value 做笛卡尔组合。
      // Alice 有 2 个分数、2 个社团，因此产生 4 条记录；cogroup 仍只有一条 Alice。
      val joined: RDD[(String, (Int, String))] = scores.join(clubs)
      checkResult(
        "join: one row per score/club combination",
        joined.collect().sorted.toSeq,
        Seq(
          ("Alice", (80, "Chess")),
          ("Alice", (80, "Music")),
          ("Alice", (90, "Chess")),
          ("Alice", (90, "Music")),
          ("Bob", (70, "Sports")),
          ("Bob", (70, "Sports"))
        )
      )

      // 6. groupBy：按自定义函数分组，value 保留完整的原元素，而非仅保留原 value。
      val byInitial: RDD[(Char, Iterable[(String, Int)])] =
        scores.groupBy { case (name, _) => name.head }
      checkResult(
        "groupBy: complete pairs by name initial",
        byInitial.mapValues(_.toList.sorted).collect().sortBy(_._1).toSeq,
        Seq(
          ('A', List(("Alice", 80), ("Alice", 90))),
          ('B', List(("Bob", 70), ("Bob", 70))),
          ('C', List(("Cindy", 100)))
        )
      )

      // 以上都是惰性 Transformation，由 collect 等 Action 触发执行。
      // 分组通常涉及 Shuffle；collect 和转 List 仅适合此处的小规模学习数据。
      println("All PairRDD grouping checks passed.")
    } finally {
      spark.stop()
    }
  }
}
