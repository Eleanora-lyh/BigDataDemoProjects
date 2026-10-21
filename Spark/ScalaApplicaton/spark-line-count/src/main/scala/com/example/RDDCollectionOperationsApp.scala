package com.example

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

// 在项目目录运行：sbt "runMain com.example.RDDCollectionOperationsApp"
object RDDCollectionOperationsApp {

  private def checkResult(
      label: String,
      rdd: RDD[Int],
      expected: Seq[Int]
  ): Unit = {
    // collect 是 Action；这里在 Driver 排序，只为方便观察小数据，不会改变 RDD。
    val actual = rdd.collect().sorted.toSeq
    println(s"$label: ${actual.mkString("[", ", ", "]")}")
    assert(actual == expected, s"$label: expected $expected, got $actual")
  }

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("RDD Collection Operations")
      .master("local[2]")
      .config("spark.ui.enabled", "false")
      .getOrCreate()

    try {
      val sc = spark.sparkContext
      sc.setLogLevel("WARN")

      val left = sc.parallelize(Seq(1, 2, 3), numSlices = 2)
      val right = sc.parallelize(Seq(3, 4, 5), numSlices = 2)

      println("left  = [1, 2, 3]")
      println("right = [3, 4, 5]")

      // union 合并两个 RDD，但不会像数学集合的并集一样自动去重。
      checkResult("union", left.union(right), Seq(1, 2, 3, 3, 4, 5))

      checkResult("distinct", left.distinct(), Seq(1, 2, 3))
      checkResult("union + distinct", left.union(right).distinct(), Seq(1, 2, 3, 4, 5))

      // intersection 返回两边共有的元素，并去重。
      checkResult("intersection", left.intersection(right), Seq(3))

      // subtract 移除另一边出现过的元素，但保留剩余元素原有的重复次数。
      checkResult("left subtract right", left.subtract(right), Seq(1, 2))
      checkResult("right subtract left", right.subtract(left), Seq(4, 5))

      // cartesian 生成所有可能的配对；数据量为两边元素数量的乘积。
      val numbers = sc.parallelize(Seq(1, 2), numSlices = 2)
      val letters = sc.parallelize(Seq("A", "B"), numSlices = 2)
      val cartesianResult = numbers.cartesian(letters).collect().sorted.toSeq
      println(s"cartesian: ${cartesianResult.mkString("[", ", ", "]")}")
      assert(cartesianResult == Seq((1, "A"), (1, "B"), (2, "A"), (2, "B")))

      // zip 按各分区内的位置配对；要求分区数相同，且对应分区的元素数也相同。
      val labels = sc.parallelize(Seq("A", "B", "C"), numSlices = 2)
      val zipResult = left.zip(labels).collect().toSeq
      println(s"zip: ${zipResult.mkString("[", ", ", "]")}")
      assert(zipResult == Seq((1, "A"), (2, "B"), (3, "C")))

      println("All RDD collection checks passed.")
    } finally {
      spark.stop()
    }
  }
}
