package com.example

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

// 在项目目录运行：sbt "runMain com.example.PairRDDCollectinoOperationApp"
object PairRDDCollectinoOperationApp {

  private def checkResult[T](label: String, actual: T, expected: T): Unit = {
    println(s"$label: $actual")
    assert(actual == expected, s"$label: expected $expected, got $actual")
  }

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Pair RDD Collection Operation")
      .master("local[2]")
      .config("spark.ui.enabled", "false")
      .getOrCreate()

    try {
      val sc = spark.sparkContext
      sc.setLogLevel("WARN")

      // 普通 RDD 的 reduce/fold/aggregate 是 Action，返回 Driver 端的聚合结果。
      // Pair RDD 的 *ByKey 是惰性 Transformation，返回 RDD，每个 key 独立聚合。
      // 普通 RDD 没有 combine()；combineByKey 是更通用的按键聚合接口。
      val sales: RDD[(String, Int)] = sc.parallelize(
        Seq(("Apple", 10), ("Banana", 20), ("Apple", 30), ("Banana", 40)),
        numSlices = 2
      )
      println("sales = [(Apple,10), (Banana,20), (Apple,30), (Banana,40)]")
      val values = sales.values
      val expectedTotals = Seq(("Apple", 40), ("Banana", 60))
      val expectedStats = Seq(("Apple", (40, 2)), ("Banana", (60, 2)))
      val expectedAverages = Seq(("Apple", 20.0), ("Banana", 30.0))

      // 1. reduce：不区分商品，对所有销售额聚合，10 + 20 + 30 + 40 = 100。
      checkResult("reduce: total sales", values.reduce(_ + _), 100)

      // 2. reduceByKey：Apple = 10 + 30，Banana = 20 + 40，无需初始值。
      // 函数应满足结合律和交换律；Spark 会先在分区内合并，再跨分区合并。
      val reduced: RDD[(String, Int)] = sales.reduceByKey(_ + _)
      // collect 才触发执行；这里只拉取小数据，并在 Driver 排序以稳定展示和校验。
      checkResult("reduceByKey: totals", reduced.collect().sortBy(_._1).toSeq, expectedTotals)

      // 3. fold：整体使用零值聚合。零值会参与各分区计算及最终合并。
      // 零值应满足 op(zeroValue, x) = x，例如加法用 0，乘法用 1。
      checkResult("fold: total sales", values.fold(0)(_ + _), 100)

      // 4. foldByKey：每个商品独立使用零值聚合。
      // 零值可能被使用多次，必须是合并函数的单位元；合并函数仍需满足结合律和交换律。
      val folded: RDD[(String, Int)] = sales.foldByKey(0)(_ + _)
      checkResult("foldByKey: totals", folded.collect().sortBy(_._1).toSeq, expectedTotals)

      // 5. aggregate / aggregateByKey：累加器 (销售额总和, 记录数) 的类型不同于 value。
      // seqOp 在分区内累加 value；combOp 合并不同分区产生的累加器。
      val totalStats = values.aggregate((0, 0))(
        (acc, value) => (acc._1 + value, acc._2 + 1),
        (left, right) => (left._1 + right._1, left._2 + right._2)
      )
      checkResult("aggregate: (sum, count)", totalStats, (100, 4))
      checkResult("aggregate: average", totalStats._1.toDouble / totalStats._2, 25.0)

      val aggregated: RDD[(String, (Int, Int))] = sales.aggregateByKey((0, 0))(
        (acc, value) => (acc._1 + value, acc._2 + 1),
        (left, right) => (left._1 + right._1, left._2 + right._2)
      )
      checkResult("aggregateByKey: (sum, count)", aggregated.collect().sortBy(_._1).toSeq, expectedStats)
      val aggregateAverages = aggregated.mapValues { case (sum, count) => sum.toDouble / count }
      checkResult(
        "aggregateByKey + mapValues: averages",
        aggregateAverages.collect().sortBy(_._1).toSeq,
        expectedAverages
      )

      // 6. combineByKey：更通用的按键聚合，累加器类型 C 可以不同于 value 类型 V。
      // createCombiner：每个分区中某 key 的第一个 value 创建 (销售额, 1)。
      // mergeValue：把该分区中同 key 的后续 value 加入累加器。
      // mergeCombiners：合并不同分区中同 key 的累加器。
      // 不能直接合并各分区平均值，因为各分区的记录数可能不同。
      val combined: RDD[(String, (Int, Int))] = sales.combineByKey[(Int, Int)](
        (value: Int) => (value, 1),
        (acc: (Int, Int), value: Int) => (acc._1 + value, acc._2 + 1),
        (left: (Int, Int), right: (Int, Int)) =>
          (left._1 + right._1, left._2 + right._2)
      )
      checkResult("combineByKey: (sum, count)", combined.collect().sortBy(_._1).toSeq, expectedStats)
      val averages = combined.mapValues { case (sum, count) => sum.toDouble / count }
      checkResult(
        "combineByKey + mapValues: averages",
        averages.collect().sortBy(_._1).toSeq,
        expectedAverages
      )

      // xxxByKey 可逻辑上理解为按 key 分组后聚合，但不等于先执行 groupByKey。
      // 这些聚合会先在分区内局部合并，再通过 Shuffle 合并，减少传输和内存开销。
      println("All PairRDD aggregation checks passed.")
    } finally {
      spark.stop()
    }
  }
}
