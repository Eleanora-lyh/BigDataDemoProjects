package com.example

import org.apache.spark.HashPartitioner
import org.apache.spark.sql.SparkSession

// 在项目目录运行：sbt "runMain com.example.PairRDD_4ActionOperationsApp"
object PairRDD_4ActionOperationsApp {

  private def checkResult[T](label: String, actual: T, expected: T): Unit = {
    println(s"$label: $actual")
    assert(actual == expected, s"$label: expected $expected, got $actual")
  }

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Pair RDD Action Operations")
      .master("local[2]")
      .config("spark.ui.enabled", "false")
      .getOrCreate()

    try {
      val sc = spark.sparkContext
      sc.setLogLevel("WARN")
      val rdd = sc.parallelize(Seq((1, 2), (3, 4), (3, 6)), numSlices = 2)

      checkResult("countByKey", rdd.countByKey().toMap, Map(1 -> 1L, 3 -> 2L))

      // Map 的 Key 唯一，重复 Key 的 Value 会被覆盖，不能依赖哪一个 Value 被保留。
      val collected = rdd.collectAsMap()
      println(s"collectAsMap: $collected")
      checkResult("collectAsMap: key set", collected.keySet.toSet, Set(1, 3))
      checkResult("collectAsMap: unique key", collected(1), 2)
      checkResult("collectAsMap: one value survives for key 3", Set(4, 6).contains(collected(3)), true)
      val totals = rdd.reduceByKey(_ + _).collectAsMap()
      checkResult("reduceByKey + collectAsMap: no ambiguous overwrite", totals.toMap, Map(1 -> 2, 3 -> 10))

      checkResult("lookup: all values for key 3", rdd.lookup(3).sorted, Seq(4, 6))
      checkResult("lookup: missing key", rdd.lookup(2), Seq.empty[Int])
      val partitioned = rdd.partitionBy(new HashPartitioner(2))
      checkResult("lookup with known partitioner", partitioned.lookup(3).sorted, Seq(4, 6))

      // 对应排序小节的 Top N：返回 Driver 端 Array，不返回 RDD。
      // 只比较 Key 时，相同 Key 的记录没有确定的先后顺序。
      val ranked = sc.parallelize(
        List(("b", 2), ("a", 3), ("a", 1), ("b", 1), ("a", 2)),
        numSlices = 2
      )
      val byKey: Ordering[(String, Int)] = Ordering.by[(String, Int), String](_._1)
      val takeMin2 = ranked.takeOrdered(2)(byKey)
      val takeMax3 = ranked.top(3)(byKey)
      println(s"takeOrdered by key: ${takeMin2.toSeq}")
      println(s"top by key: ${takeMax3.toSeq}")
      checkResult("takeOrdered: ascending keys", takeMin2.map(_._1).toSeq, Seq("a", "a"))
      checkResult("top: descending keys", takeMax3.map(_._1).toSeq, Seq("b", "b", "a"))

      // 增加 Value 作为次级字段，才能确定精确的记录顺序。
      val byKeyThenValue = Ordering.Tuple2(Ordering.String, Ordering.Int)
      checkResult(
        "takeOrdered: key then value",
        ranked.takeOrdered(2)(byKeyThenValue).toSeq,
        Seq(("a", 1), ("a", 2))
      )
      checkResult(
        "top: key then value",
        ranked.top(3)(byKeyThenValue).toSeq,
        Seq(("b", 2), ("b", 1), ("a", 3))
      )
      checkResult("takeOrdered: zero rows", ranked.takeOrdered(0)(byKey).toSeq, Seq.empty[(String, Int)])
      checkResult("top: empty RDD", sc.emptyRDD[(String, Int)].top(3)(byKey).toSeq, Seq.empty[(String, Int)])

      // countByKey/collectAsMap 会把所有 Key 的统计或记录取回 Driver，只适合小结果集。
      println("All PairRDD action checks passed.")
    } finally {
      spark.stop()
    }
  }
}
