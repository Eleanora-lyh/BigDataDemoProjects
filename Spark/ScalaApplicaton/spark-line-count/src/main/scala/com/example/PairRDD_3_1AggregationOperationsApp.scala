package com.example

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

// 在项目目录运行：sbt "runMain com.example.PairRDD_3_1AggregationOperationsApp"
object PairRDD_3_1AggregationOperationsApp {

  private def checkResult[T](label: String, actual: T, expected: T): Unit = {
    println(s"$label: $actual")
    assert(actual == expected, s"$label: expected $expected, got $actual")
  }

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Pair RDD Aggregation Operations")
      .master("local[2]")
      .config("spark.ui.enabled", "false")
      .getOrCreate()

    try {
      val sc = spark.sparkContext
      sc.setLogLevel("WARN")

      // 普通 RDD 的 reduce/fold/aggregate 是 Action，返回 Driver 端的聚合结果。
      // Pair RDD 的 *ByKey 是惰性 Transformation，返回 RDD，每个 key 独立聚合。
      // 普通 RDD 没有 combine()；combineByKey 是更通用的按键聚合接口。
      val sales = sc.parallelize(
        Seq(
          ("Apple", 10),
          ("Banana", 20),
          ("Apple", 30),
          ("Banana", 40)
        ),
        numSlices = 2
      )
      println("sales = [(Apple,10), (Banana,20), (Apple,30), (Banana,40)]")
      val expectedTotals = Seq(("Apple", 40), ("Banana", 60))
      val expectedStats = Seq(("Apple", (40, 2)), ("Banana", (60, 2)))
      val expectedAverages = Seq(("Apple", 20.0), ("Banana", 30.0))

      // 局部作用域与文章 3.1 的四个小节对应，允许分别使用 total/result/averages。
      // collect 触发执行；这里只拉取小数据，并在 Driver 排序以稳定展示和校验。

      // 1. reduceByKey：对比整体聚合与按 Key 聚合，无需初始值。
      // 合并函数应满足结合律和交换律；Spark 可先在分区内合并，再跨分区合并。
      locally {
        val total = sales.values.reduce((x, y) => x + y)
        checkResult("reduce: total sales", total, 100)

        val result = sales.reduceByKey((x, y) => x + y)
        checkResult("reduceByKey: totals", result.collect().sortBy(_._1).toSeq, expectedTotals)
      }

      // 2. foldByKey：每个 Key 使用零值聚合；fold 则聚合整个 values RDD。
      // 零值可能参与多次计算，应为合并函数的单位元，例如加法用 0，乘法用 1。
      locally {
        val total = sales.values.fold(0)((x, y) => x + y)
        checkResult("fold: total sales", total, 100)

        val result = sales.foldByKey(0)((x, y) => x + y)
        checkResult("foldByKey: totals", result.collect().sortBy(_._1).toSeq, expectedTotals)
      }

      // 3. aggregateByKey：用 (sum, count) 统计每个商品，与整体统计对比。
      // seqOp 在分区内累加 Value；combOp 合并不同分区产生的累加器。
      locally {
        val result = sales.values.aggregate((0, 0))(
          (acc, value) => (acc._1 + value, acc._2 + 1),
          (left, right) => (left._1 + right._1, left._2 + right._2)
        )
        val average = result._1.toDouble / result._2
        checkResult("aggregate: (sum, count)", result, (100, 4))
        checkResult("aggregate: average", average, 25.0)
      }

      locally {
        val result = sales.aggregateByKey((0, 0))(
          (acc, value) => (acc._1 + value, acc._2 + 1),
          (left, right) => (left._1 + right._1, left._2 + right._2)
        )
        val averages = result.mapValues {
          case (sum, count) => sum.toDouble / count
        }
        checkResult("aggregateByKey: (sum, count)", result.collect().sortBy(_._1).toSeq, expectedStats)
        checkResult(
          "aggregateByKey + mapValues: averages",
          averages.collect().sortBy(_._1).toSeq,
          expectedAverages
        )
      }

      // 4. combineByKey：不直接提供固定零值，而是从首个 Value 创建聚合器。
      // createCombiner：每个分区中某 Key 的首个 Value 创建 (销售额, 1)。
      // mergeValue：将分区内同 Key 的后续 Value 加入累加器。
      // mergeCombiners：合并不同分区中同 Key 的累加器。
      locally {
        val combined: RDD[(String, (Int, Int))] = sales.combineByKey[(Int, Int)](
          (value: Int) => (value, 1),
          (acc: (Int, Int), value: Int) => (acc._1 + value, acc._2 + 1),
          (left: (Int, Int), right: (Int, Int)) =>
            (left._1 + right._1, left._2 + right._2)
        )
        val averages = combined.mapValues { case (sum, count) => sum.toDouble / count }
        checkResult("combineByKey: (sum, count)", combined.collect().sortBy(_._1).toSeq, expectedStats)
        checkResult(
          "combineByKey + mapValues: averages",
          averages.collect().sortBy(_._1).toSeq,
          expectedAverages
        )
      }

      // xxxByKey 可逻辑上理解为按 key 分组后聚合，但不等于先执行 groupByKey。
      // 这些聚合可先在分区内局部合并，再在 Executor 端跨分区合并；collect 才将结果取回 Driver。
      // 计算平均值时保留 (sum, count)，不能直接合并记录数可能不同的各分区平均值。
      println("All PairRDD aggregation checks passed.")
    } finally {
      spark.stop()
    }
  }
}
