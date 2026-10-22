package com.example

import org.apache.spark.sql.SparkSession

// 在项目目录运行：sbt "runMain com.example.RDDActionOperationsApp"
object RDDActionOperationsApp {
  //打印实际结果 + 验证结果
  private def checkResult[T](label: String, actual: T, expected: T): Unit = {
    println(s"$label: $actual")
    assert(actual == expected, s"$label: expected $expected, got $actual")
  }

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("RDD Action Operations")
      .master("local[2]")
      .config("spark.ui.enabled", "false")
      .getOrCreate()

    try {
      val sc = spark.sparkContext
      sc.setLogLevel("WARN")

      // RDD 不是数学集合，重复的 3 会保留；两个分区分别为 [1, 2] 和 [3, 3]。
      val rdd = sc.parallelize(Seq(1, 2, 3, 3), numSlices = 2)
      println("RDD = [1, 2, 3, 3], partitions = 2")

      // Action 触发计算；以下返回值（除 foreach 类操作外）都回到 Driver。
      // collect 拉取全部数据，仅适合小数据；不要对大 RDD 随意使用。
      checkResult("collect", rdd.collect().toSeq, Seq(1, 2, 3, 3))
      checkResult("count", rdd.count(), 4L)
      checkResult("first", rdd.first(), 1)
      checkResult("take(2)", rdd.take(2).toSeq, Seq(1, 2))

      // first/take 按当前分区及分区内顺序取数，不代表排序后的最小值。
      // takeOrdered 取最小的 n 个，top 取最大的 n 个；两者都保留重复值。
      checkResult("takeOrdered(3)", rdd.takeOrdered(3).toSeq, Seq(1, 2, 3))
      checkResult("top(2)", rdd.top(2).toSeq, Seq(3, 3))
      checkResult("min", rdd.min(), 1)
      checkResult("max", rdd.max(), 3)

      // reduce 的函数应满足结合律和交换律，不能依赖执行顺序；空 RDD 会报错。
      checkResult("reduce(sum)", rdd.reduce(_ + _), 9)

      // fold 会在每个分区及最终合并时使用初始值，应选运算的单位元（加法用 0）。
      checkResult("fold(0)(sum)", rdd.fold(0)(_ + _), 9)

      // aggregate 可使用不同于元素的累加类型：这里同时计算 (总和, 个数)。
      // seqOp 在分区内累加，combOp 合并各分区结果。
      val sumAndCount = rdd.aggregate((0, 0L))(
        (acc, value) => (acc._1 + value, acc._2 + 1L),
        (left, right) => (left._1 + right._1, left._2 + right._2)
      )
      checkResult("aggregate(sum, count)", sumAndCount, (9, 4L))
      checkResult("average from aggregate", sumAndCount._1.toDouble / sumAndCount._2, 2.25)

      // countByValue 返回 Driver 端 Map，键的种类很多时需注意 Driver 内存。
      val counts = rdd.countByValue().toMap
      checkResult("countByValue", counts, Map(1 -> 1L, 2 -> 1L, 3 -> 2L))

      // takeSample 返回随机样本；无放回按元素位置抽样，因此仍可能抽到两个 3。
      // seed 用于本示例复现；不把随机样本的具体顺序作为断言条件。
      val sample = rdd.takeSample(withReplacement = false, num = 3, seed = 42L).toSeq
      println(s"takeSample(false, 3, 42): ${sample.mkString("[", ", ", "]")}")
      assert(sample.size == 3, s"Expected 3 sampled elements, got $sample")
      val sampleCounts = sample.groupBy(identity).map { case (value, values) =>
        value -> values.size.toLong
      }
      assert(
        sampleCounts.forall { case (value, count) => count <= counts.getOrElse(value, 0L) },
        s"Sample contains unavailable elements: $sample"
      )

      // foreach 的函数在 Executor 执行，返回 Unit；打印顺序不保证。
      // 集群模式下输出在 Executor 日志，不一定显示在 Driver 控制台。
      println("foreach (Executor output, order may vary):")
      rdd.foreach(value => println(s"  element = $value"))

      // foreachPartition 每个分区调用一次，常用于在分区内复用数据库连接等资源。
      // 不要在这些闭包里修改 Driver 的普通变量；外部写入还需考虑任务重试及幂等性。
      println("foreachPartition (Executor output, order may vary):")
      rdd.foreachPartition { values =>
        println(s"  partition = ${values.mkString("[", ", ", "]")}")
      }

      // collect 后的 foreach 是 Scala 集合遍历，在 Driver 执行，不是 RDD Action。
      println("collect().foreach (Driver output):")
      rdd.collect().foreach(value => println(s"  element = $value"))

      println("All RDD action checks passed.")
    } finally {
      spark.stop()
    }
  }
}
