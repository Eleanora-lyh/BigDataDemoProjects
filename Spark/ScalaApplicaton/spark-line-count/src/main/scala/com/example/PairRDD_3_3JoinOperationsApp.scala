package com.example

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

// 在项目目录运行：sbt "runMain com.example.PairRDD_3_3JoinOperationsApp"
object PairRDD_3_3JoinOperationsApp {

  private def checkResult[T](label: String, actual: T, expected: T): Unit = {
    println(s"$label: $actual")
    assert(actual == expected, s"$label: expected $expected, got $actual")
  }

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Pair RDD Join")
      .master("local[2]")
      .config("spark.ui.enabled", "false")
      .getOrCreate()

    try {
      val sc = spark.sparkContext
      sc.setLogLevel("WARN")

      // 对应文章中 A 的两条销售记录与两条成本记录的多对多连接。
      val sales = sc.parallelize(Seq(("A", 10), ("A", 20)), numSlices = 2)
      val costs = sc.parallelize(Seq(("A", 7), ("A", 8)), numSlices = 2)
      checkResult(
        "sales.join(costs): many-to-many",
        sales.join(costs).collect().sorted.toSeq,
        Seq(("A", (10, 7)), ("A", (10, 8)), ("A", (20, 7)), ("A", (20, 8)))
      )

      // 两个 RDD 的 key 类型相同，value 类型可以不同；连接条件是 key 相等。
      val scores: RDD[(String, Int)] = sc.parallelize(
        Seq(("Alice", 80), ("Alice", 90), ("Bob", 70), ("Bob", 70), ("Cindy", 100)),
        numSlices = 2
      )
      val clubs: RDD[(String, String)] = sc.parallelize(
        Seq(("Alice", "Chess"), ("Alice", "Music"), ("Bob", "Sports"), ("David", "Art")),
        numSlices = 2
      )
      println("scores (left) = [(Alice,80), (Alice,90), (Bob,70), (Bob,70), (Cindy,100)]")
      println("clubs (right) = [(Alice,Chess), (Alice,Music), (Bob,Sports), (David,Art)]")

      // 1. join：内连接，仅保留双方都有的 key。
      // RDD[(K, V)].join(RDD[(K, W)]) -> RDD[(K, (V, W))]。
      // 同一个 key 左侧有 m 条、右侧有 n 条，就产生 m * n 条，不会自动去重。
      val inner: RDD[(String, (Int, String))] = scores.join(clubs)
      val expectedInner = Seq(
        ("Alice", (80, "Chess")),
        ("Alice", (80, "Music")),
        ("Alice", (90, "Chess")),
        ("Alice", (90, "Music")),
        ("Bob", (70, "Sports")),
        ("Bob", (70, "Sports"))
      )
      // Join 不保证输出顺序；这里只在 Driver 排序，让展示和断言结果稳定。
      checkResult("join: matched pairs", inner.collect().sorted.toSeq, expectedInner)

      // 2. leftOuterJoin：保留左侧所有记录；右侧无匹配时为 None，有匹配时为 Some。
      // 返回 RDD[(K, (V, Option[W]))]，Cindy 没有社团，但分数仍被保留。
      val left: RDD[(String, (Int, Option[String]))] = scores.leftOuterJoin(clubs)
      val expectedLeft: Seq[(String, (Int, Option[String]))] =
        expectedInner.map { case (name, (score, club)) =>
          (name, (score, Some(club)))
        } :+ ("Cindy", (100, None))
      checkResult("leftOuterJoin: all scores", left.collect().sorted.toSeq, expectedLeft)

      // 3. rightOuterJoin：保留右侧所有记录；左侧无匹配时为 None。
      // 返回 RDD[(K, (Option[V], W))]，David 没有分数，但社团记录仍被保留。
      // 注意 value 顺序仍然是 (左侧分数, 右侧社团)，不会因为右连接而交换。
      val right: RDD[(String, (Option[Int], String))] = scores.rightOuterJoin(clubs)
      val expectedRight: Seq[(String, (Option[Int], String))] =
        expectedInner.map { case (name, (score, club)) =>
          (name, (Some(score), club))
        } :+ ("David", (None, "Art"))
      checkResult("rightOuterJoin: all clubs", right.collect().sorted.toSeq, expectedRight)

      // 4. fullOuterJoin：保留双方所有 key，左右 value 都用 Option 表示。
      // 返回 RDD[(K, (Option[V], Option[W]))]；Cindy 缺右侧，David 缺左侧。
      val full: RDD[(String, (Option[Int], Option[String]))] =
        scores.fullOuterJoin(clubs)
      val expectedFull: Seq[(String, (Option[Int], Option[String]))] =
        expectedInner.map { case (name, (score, club)) =>
          (name, (Some(score), Some(club)))
        } ++ Seq(
          ("Cindy", (Some(100), None)),
          ("David", (None, Some("Art")))
        )
      checkResult("fullOuterJoin: all keys", full.collect().sorted.toSeq, expectedFull)

      // 5. 使用模式匹配处理 Option，不要直接 .get，也不要把缺失分数当成 0。
      val descriptions = full.mapValues {
        case (Some(score), Some(club)) => s"score=$score, club=$club"
        case (Some(score), None) => s"score=$score, no matching club"
        case (None, Some(club)) => s"no matching score, club=$club"
        case (None, None) =>
          throw new IllegalStateException("fullOuterJoin cannot produce two missing sides")
      }
      checkResult(
        "fullOuterJoin: describe unmatched records",
        descriptions.filter { case (name, _) => name == "Cindy" || name == "David" }
          .collect().sorted.toSeq,
        Seq(
          ("Cindy", "score=100, no matching club"),
          ("David", "no matching score, club=Art")
        )
      )

      // 6. subtractByKey：找出左侧有、右侧没有的 key，类似按 key 做左反连接。
      // 只比较 key，不比较 value；返回原始左侧记录，不是外连接的 Option 结构。
      val unmatched: RDD[(String, Int)] = scores.subtractByKey(clubs)
      checkResult(
        "subtractByKey: scores without clubs",
        unmatched.collect().sorted.toSeq,
        Seq(("Cindy", 100))
      )

      // 7. 空 RDD 边界：内连接没有结果，左外连接仍保留全部左侧记录。
      val noClubs = sc.emptyRDD[(String, String)]
      checkResult("join with empty RDD: row count", scores.join(noClubs).count(), 0L)
      val expectedWithNoClubs: Seq[(String, (Int, Option[String]))] = Seq(
        ("Alice", (80, None)),
        ("Alice", (90, None)),
        ("Bob", (70, None)),
        ("Bob", (70, None)),
        ("Cindy", (100, None))
      )
      checkResult(
        "leftOuterJoin with empty RDD",
        scores.leftOuterJoin(noClubs).collect().sorted.toSeq,
        expectedWithNoClubs
      )

      // Join 是惰性 Transformation，由 collect/count 等 Action 触发执行。
      // 通常涉及 Shuffle；复用兼容的分区器可减少重分区，但不能避免多对多结果膨胀。
      // 热点 key 可能导致数据倾斜；collect 仅用于这里的小数据，不适合生产大结果集。
      println("All PairRDD join checks passed.")
    } finally {
      spark.stop()
    }
  }
}
