package com.example

import org.apache.spark.HashPartitioner
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.reflect.classTag

// 在项目目录运行：sbt "runMain com.example.PairRDD_3_4SortingOperationsApp"
object PairRDD_3_4SortingOperationsApp {

  private def checkResult[T](label: String, actual: T, expected: T): Unit = {
    println(s"$label: $actual")
    assert(actual == expected, s"$label: expected $expected, got $actual")
  }

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Pair RDD Sorting")
      .master("local[2]")
      .config("spark.ui.enabled", "false")
      .getOrCreate()

    try {
      val sc = spark.sparkContext
      sc.setLogLevel("WARN")

      val rdd = sc.parallelize(
        Seq(
          (3, "C"),
          (1, "A"),
          (4, "D"),
          (2, "B")
        )
      )
      val expectedAscending = Seq((1, "A"), (2, "B"), (3, "C"), (4, "D"))
      val expectedDescending = expectedAscending.reverse
      checkResult(
        "original data",
        rdd.collect().toSeq,
        Seq((3, "C"), (1, "A"), (4, "D"), (2, "B"))
      )

      // 1. sortByKey：按 Key 全局排序，默认升序，不改变每条记录的 value。
      // Scala 接口是 sortByKey(ascending, numPartitions)，没有 partitioner 参数。
      // 底层使用 RangePartitioner，让各分区的 Key 范围也按指定顺序排列。
      val sortByKeyAscending: RDD[(Int, String)] = rdd.sortByKey()
      checkResult("sortByKey: ascending", sortByKeyAscending.collect().toSeq, expectedAscending)

      val sortByKeyDescending: RDD[(Int, String)] = rdd.sortByKey(
        ascending = false,
        numPartitions = 4
      )
      checkResult("sortByKey: descending", sortByKeyDescending.collect().toSeq, expectedDescending)
      checkResult("sortByKey: partition count", sortByKeyDescending.getNumPartitions, 4)

      // 2. 指定 HashPartitioner 时，用 repartitionAndSortWithinPartitions。
      // 分区数量由分区器决定：HashPartitioner(2) 表示 2 个分区，而不是 4 个。
      // 此接口没有 ascending 参数，降序需要在局部作用域提供反向 Ordering。
      val sortByKeyInPartition: RDD[(Int, String)] = {
        implicit val descendingOrdering: Ordering[Int] = Ordering.Int.reverse
        rdd.repartitionAndSortWithinPartitions(new HashPartitioner(2))
      }
      checkResult("hash partitioning: partition count", sortByKeyInPartition.getNumPartitions, 2)
      checkResult(
        "hash partitioning: partitioner",
        sortByKeyInPartition.partitioner,
        Some(new HashPartitioner(2))
      )
      val partitionContents = sortByKeyInPartition.mapPartitionsWithIndex { (index, rows) =>
        Iterator((index, rows.toList))
      }.collect().toSeq
      // 对这些正整数，分区号为 key % 2：偶数在分区 0，奇数在分区 1。
      checkResult(
        "descending keys within each hash partition",
        partitionContents,
        Seq((0, List((4, "D"), (2, "B"))), (1, List((3, "C"), (1, "A"))))
      )
      // 拼接后是 4,2,3,1，说明“每个分区内有序”不等于“全局有序”。
      checkResult(
        "hash partitions are not globally sorted",
        partitionContents.flatMap(_._2),
        Seq((4, "D"), (2, "B"), (3, "C"), (1, "A"))
      )

      // 3. 自定义 Key 排序：偶数优先于奇数，同一组内按 Key 升序。
      // 比较依据为 (key % 2, key)，结果是 2,4,1,3，而不是普通数值顺序。
      // 显式指定元组各字段的自然顺序，避免构建规则时引用正在定义的隐式 Ordering。
      val customKeySorted: RDD[(Int, String)] = {
        implicit val evenFirstOrdering: Ordering[Int] =
          Ordering.by[Int, (Int, Int)](key => (key % 2, key))(
            Ordering.Tuple2(Ordering.Int, Ordering.Int)
          )
        rdd.sortByKey(ascending = true, numPartitions = 2)
      }
      checkResult(
        "custom key ordering: even keys first",
        customKeySorted.collect().toSeq,
        Seq((2, "B"), (4, "D"), (1, "A"), (3, "C"))
      )

      // 4. sortBy：提取任意排序字段，输出仍保留原始 (key, value)。
      // _._2 提取 value；sortByKey 始终使用 key，不能直接指定按 value 排序。
      val sortByValueAscending = rdd.sortBy(_._2, ascending = true, numPartitions = 2)
      val sortByValueDescending = rdd.sortBy(_._2, ascending = false, numPartitions = 2)
      checkResult("sortBy value: ascending", sortByValueAscending.collect().toSeq, expectedAscending)
      checkResult("sortBy value: descending", sortByValueDescending.collect().toSeq, expectedDescending)

      // 原始数据的 Key 与 Value 顺序恰好一致；派生出重复 Value，观察多字段排序。
      val categories = rdd.mapValues(value => if (value <= "B") "Low" else "High")
      // 先按 Value 升序，再按 Key 降序；元组默认按第一个字段、第二个字段依次比较。
      // 这里 Key 只有 1 到 4，可安全取负；任意 Int 数据应避免 Int.MinValue 取负溢出。
      val sortByMultipleFields = categories.sortBy(
        { case (key, value) => (value, -key) },
        ascending = true,
        numPartitions = 2
      )
      checkResult(
        "sortBy multiple fields: value ascending, key descending",
        sortByMultipleFields.collect().toSeq,
        Seq((4, "High"), (3, "High"), (2, "Low"), (1, "Low"))
      )

      // 对应文章的重复 Key 数据，验证 Key 相同时按 Value 降序。
      locally {
        val rdd = sc.parallelize(
          Seq((3, "a"), (1, "b"), (2, "c"), (2, "d"), (5, "e")),
          numSlices = 2
        )
        val expectedByValue = Seq((3, "a"), (1, "b"), (2, "c"), (2, "d"), (5, "e"))
        val sortByValueAsc = rdd.sortBy { case (_, value) => value }
        val sortByValueAscAbb = rdd.sortBy(_._2)
        val sortByValueAscLam = rdd.sortBy(pair => pair._2)
        checkResult("value sorting: pattern matching", sortByValueAsc.collect().toSeq, expectedByValue)
        checkResult("value sorting: placeholder", sortByValueAscAbb.collect().toSeq, expectedByValue)
        checkResult("value sorting: lambda", sortByValueAscLam.collect().toSeq, expectedByValue)
        checkResult(
          "value sorting: descending",
          rdd.sortBy(_._2, ascending = false).collect().toSeq,
          expectedByValue.reverse
        )

        val expectedMixed = Seq((1, "b"), (2, "d"), (2, "c"), (3, "a"), (5, "e"))
        val sortByKeyAscValueDesc1 = rdd.sortBy(
          pair => (pair._1, pair._2),
          ascending = true
        )(Ordering.Tuple2(Ordering.Int, Ordering.String.reverse), classTag[(Int, String)])
        checkResult("mixed sorting: explicit Ordering", sortByKeyAscValueDesc1.collect().toSeq, expectedMixed)

        locally {
          val ordering = Ordering.Tuple2(Ordering.Int, Ordering.String.reverse)
          val sortByKeyAscValueDesc2 = rdd.sortBy(
            pair => (pair._1, pair._2),
            ascending = true
          )(ordering, classTag[(Int, String)])
          checkResult("mixed sorting: named Ordering", sortByKeyAscValueDesc2.collect().toSeq, expectedMixed)
        }

        locally {
          implicit val ordering: Ordering[(Int, String)] =
            Ordering.Tuple2(Ordering.Int, Ordering.String.reverse)
          val sortByKeyAscValueDesc3 = rdd.sortBy(pair => (pair._1, pair._2))
          checkResult("mixed sorting: implicit Ordering", sortByKeyAscValueDesc3.collect().toSeq, expectedMixed)
        }

        val sortByKeyAscValueDesc4 = rdd.sortBy(
          pair => (-pair._1, pair._2),
          ascending = false
        )
        checkResult("mixed sorting: negated small integer key", sortByKeyAscValueDesc4.collect().toSeq, expectedMixed)

        val withinPartitions = {
          implicit val descendingOrdering: Ordering[Int] = Ordering.Int.reverse
          rdd.repartitionAndSortWithinPartitions(new HashPartitioner(2))
        }
        val partitions = withinPartitions.glom().collect().map(_.toSeq).toSeq
        checkResult("article hash partition 0: equal keys", partitions(0).toSet, Set((2, "c"), (2, "d")))
        checkResult("article hash partition 1: descending keys", partitions(1), Seq((5, "e"), (3, "a"), (1, "b")))
        checkResult("article hash partition 0: record count", partitions(0).size, 2)
      }

      // 不能在 collect 后再 .sorted 来验证 Spark 排序，那只是在 Driver 重新排序。
      // 排序通常涉及 Shuffle；相同排序字段的记录不保证稳定顺序，需要时增加次级字段。
      // collect 和分区转 List 仅用于这里的小数据；生产环境不要拉取整个大 RDD。
      println("All PairRDD sorting checks passed.")
    } finally {
      spark.stop()
    }
  }
}
