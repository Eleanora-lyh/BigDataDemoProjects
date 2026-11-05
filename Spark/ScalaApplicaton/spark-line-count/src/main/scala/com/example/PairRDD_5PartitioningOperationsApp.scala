package com.example

import org.apache.spark.{HashPartitioner, Partitioner, RangePartitioner}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

// 在项目目录运行：sbt "runMain com.example.PairRDD_5PartitioningOperationsApp"
object PairRDD_5PartitioningOperationsApp {

  final case class UserInfo(topics: Set[String])
  final case class LinkInfo(topic: String, url: String)

  final case class TopicPartitioner() extends Partitioner {
    override val numPartitions: Int = 3

    override def getPartition(key: Any): Int = key match {
      case "spark" | "scala" => 0
      case "hive" | "sql" => 1
      case _: String => 2
      case _ => throw new IllegalArgumentException(s"Topic key must be a String: $key")
    }
  }

  private def checkResult[T](label: String, actual: T, expected: T): Unit = {
    println(s"$label: $actual")
    assert(actual == expected, s"$label: expected $expected, got $actual")
  }

  private def processNewLogs(
      userData: RDD[(Int, UserInfo)],
      events: RDD[(Int, LinkInfo)],
      partitioner: HashPartitioner
  ): Long = {
    val joined = userData.join(events, partitioner)
    checkResult("user/event join: partitioner", joined.partitioner, Some(partitioner))
    // 内连接只统计已知用户；topic 不在订阅集合中才算未订阅主题的访问。
    joined.filter { case (_, (userInfo, linkInfo)) =>
      !userInfo.topics.contains(linkInfo.topic)
    }.count()
  }

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Pair RDD Partitioning Operations")
      .master("local[2]")
      .config("spark.ui.enabled", "false")
      .getOrCreate()

    try {
      val sc = spark.sparkContext
      sc.setLogLevel("WARN")
      val pairs = sc.parallelize(List((1, 1), (2, 2), (3, 3)), numSlices = 2)
      val partitioner = new HashPartitioner(2)
      val partitioned = pairs.partitionBy(partitioner).persist()

      try {
        partitioned.count()
        checkResult("original RDD: no partitioner", pairs.partitioner, None)
        checkResult("partitionBy: partitioner", partitioned.partitioner, Some(partitioner))
        checkResult("partitioner.isDefined", partitioned.partitioner.isDefined, true)
        checkResult("partition count", partitioned.getNumPartitions, 2)
        val knownPartitioner = partitioned.partitioner.getOrElse(
          throw new IllegalStateException("partitionBy must attach a partitioner")
        )
        checkResult("partition for key 3", knownPartitioner.getPartition(3), 1)

        val partitions = partitioned.mapPartitionsWithIndex { (index, rows) =>
          Iterator((index, rows.toList.sorted))
        }.collect().toSeq
        checkResult(
          "hash partition contents",
          partitions,
          Seq((0, List((2, 2))), (1, List((1, 1), (3, 3))))
        )
        checkResult("map: discards partitioner", partitioned.map(pair => pair).partitioner, None)
        checkResult("mapValues: preserves partitioner", partitioned.mapValues(_ + 1).partitioner, Some(partitioner))
        val expanded = partitioned.flatMapValues(value => Seq(value, value + 10))
        checkResult("flatMapValues: preserves partitioner", expanded.partitioner, Some(partitioner))
        checkResult("flatMapValues: row count", expanded.count(), 6L)
        checkResult("lookup: known partition", partitioned.lookup(3), Seq(3))

        val other = sc.parallelize(Seq((1, 10), (3, 30)), numSlices = 2)
        val grouped = partitioned.cogroup(other, partitioner)
        val groupWith = partitioned.groupWith(other)
        val expectedGroups = Seq(
          (1, (List(1), List(10))),
          (2, (List(2), List.empty[Int])),
          (3, (List(3), List(30)))
        )
        checkResult(
          "cogroup",
          grouped.mapValues { case (left, right) =>
            (left.toList.sorted, right.toList.sorted)
          }.collect().sortBy(_._1).toSeq,
          expectedGroups
        )
        checkResult(
          "groupWith: alias of cogroup",
          groupWith.mapValues { case (left, right) =>
            (left.toList.sorted, right.toList.sorted)
          }.collect().sortBy(_._1).toSeq,
          expectedGroups
        )
        val joined = partitioned.join(other, partitioner)
        checkResult("join: preserves requested partitioner", joined.partitioner, Some(partitioner))
        checkResult(
          "join: values",
          joined.collect().sorted.toSeq,
          Seq((1, (1, 10)), (3, (3, 30)))
        )
        val reduced = joined.mapValues { case (left, right) => left + right }
          .reduceByKey(partitioner, (left, right) => left + right)
        checkResult("reduceByKey: compatible partitioner", reduced.partitioner, Some(partitioner))
        checkResult("reduceByKey after join", reduced.collect().sorted.toSeq, Seq((1, 11), (3, 33)))

        // 两侧分区器不同时显式指定目标，不依赖“总是选择第一个父 RDD”的错误假设。
        val differentlyPartitioned = other.partitionBy(new HashPartitioner(3))
        checkResult(
          "join: explicit target for different parent partitioners",
          partitioned.join(differentlyPartitioned, partitioner).partitioner,
          Some(partitioner)
        )
        val rangePartitioned = pairs.partitionBy(new RangePartitioner[Int, Int](2, pairs))
        checkResult("range partitioning: retains records", rangePartitioned.collect().sorted.toSeq, Seq((1, 1), (2, 2), (3, 3)))
      } finally {
        partitioned.unpersist()
      }

      // 用内存中的同构数据替代文章的外部 HDFS 输入，便于直接运行。
      val rawUsers = sc.parallelize(
        Seq((1, UserInfo(Set("spark"))), (2, UserInfo(Set("hive")))),
        numSlices = 2
      )
      val userData = rawUsers.partitionBy(partitioner).persist()
      try {
        userData.count()
        val firstBatch = sc.parallelize(
          Seq(
            (1, LinkInfo("spark", "/spark")),
            (1, LinkInfo("hive", "/hive")),
            (2, LinkInfo("hive", "/hive")),
            (99, LinkInfo("scala", "/unknown-user"))
          ),
          numSlices = 2
        )
        checkResult("first batch: unpartitioned users", processNewLogs(rawUsers, firstBatch, partitioner), 1L)
        checkResult("first batch: cached partitioned users", processNewLogs(userData, firstBatch, partitioner), 1L)
        val secondBatch = sc.parallelize(
          Seq((1, LinkInfo("scala", "/scala")), (2, LinkInfo("spark", "/spark"))),
          numSlices = 2
        )
        checkResult("second batch: reuse user partitions", processNewLogs(userData, secondBatch, partitioner), 2L)
        checkResult(
          "empty batch",
          processNewLogs(userData, sc.emptyRDD[(Int, LinkInfo)], partitioner),
          0L
        )
      } finally {
        userData.unpersist()
      }

      val topics = sc.parallelize(
        Seq(("spark", 1), ("scala", 2), ("hive", 3), ("sql", 4), ("other", 5)),
        numSlices = 2
      )
      val byTopic = topics.partitionBy(TopicPartitioner())
      val topicPartitions = byTopic.mapPartitionsWithIndex { (index, rows) =>
        Iterator((index, rows.toList.sorted))
      }.collect().toSeq
      checkResult(
        "custom topic partitioner",
        topicPartitions,
        Seq(
          (0, List(("scala", 2), ("spark", 1))),
          (1, List(("hive", 3), ("sql", 4))),
          (2, List(("other", 5)))
        )
      )
      checkResult("custom partitioner: equivalent instances", TopicPartitioner() == TopicPartitioner(), true)

      println("All PairRDD partitioning checks passed.")
    } finally {
      spark.stop()
    }
  }
}
