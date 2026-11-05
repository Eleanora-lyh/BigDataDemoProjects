package com.example

import java.nio.file.Files

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.HashPartitioner
import org.apache.spark.sql.SparkSession

// 在项目目录运行：sbt "runMain com.example.PairRDD_5PageRankApp"
object PairRDD_5PageRankApp {

  private def checkRanks(label: String, actual: Map[String, Double], expected: Map[String, Double]): Unit = {
    println(s"$label: ${actual.toSeq.sortBy(_._1)}")
    assert(actual.keySet == expected.keySet, s"$label: unexpected page set")
    expected.foreach { case (page, rank) =>
      assert(math.abs(actual(page) - rank) < 1e-10, s"$label: $page expected $rank, got ${actual(page)}")
    }
  }

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Pair RDD PageRank")
      .master("local[2]")
      .config("spark.ui.enabled", "false")
      .getOrCreate()

    try {
      val sc = spark.sparkContext
      sc.setLogLevel("WARN")
      val runDirectory = Files.createTempDirectory("pair-rdd-pagerank-")
      val fs = FileSystem.newInstanceLocal(sc.hadoopConfiguration)
      val root = new Path(runDirectory.toUri)

      try {
        val linksPath = new Path(root, "links").toString
        val ranksPath = new Path(root, "ranks").toString
        val graph = Seq(
          ("A", Seq("B", "C")),
          ("B", Seq("C")),
          ("C", Seq("A"))
        )
        sc.parallelize(graph, numSlices = 2).saveAsObjectFile(linksPath)
        val partitioner = new HashPartitioner(2)
        val links = sc.objectFile[(String, Seq[String])](linksPath)
          .partitionBy(partitioner)
          .persist()

        val pageCount = graph.size
        val damping = 0.85
        val base = (1.0 - damping) / pageCount
        var ranks = links.mapValues(_ => 1.0 / pageCount).persist()
        try {
          assert(links.count() == 3L)
          assert(ranks.partitioner == links.partitioner)
          var expected = Map("A" -> (1.0 / 3), "B" -> (1.0 / 3), "C" -> (1.0 / 3))

          // 本练习的三个页面都有入边和出边，不含需要额外处理的悬挂节点。
          for (iteration <- 1 to 10) {
            val contributions = links.join(ranks, partitioner).flatMap {
              case (_, (destinations, rank)) =>
                destinations.map(destination => (destination, rank / destinations.size))
            }
            val nextRanks = contributions.reduceByKey(partitioner, (left, right) => left + right)
              .mapValues(value => base + damping * value)
              .persist()
            nextRanks.count()
            ranks.unpersist()
            ranks = nextRanks

            // 独立的三节点递推式用于校验分布式 Join/flatMap/reduceByKey 的计算。
            expected = Map(
              "A" -> (base + damping * expected("C")),
              "B" -> (base + damping * expected("A") / 2),
              "C" -> (base + damping * (expected("A") / 2 + expected("B")))
            )
            checkRanks(s"iteration $iteration", ranks.collect().toMap, expected)
            assert(math.abs(ranks.values.sum() - 1.0) < 1e-10)
            assert(ranks.partitioner.contains(partitioner))
            if (iteration == 1) {
              checkRanks("article first iteration", ranks.collect().toMap, Map(
                "A" -> (1.0 / 3),
                "B" -> (23.0 / 120),
                "C" -> 0.475
              ))
            }
          }

          ranks.saveAsTextFile(ranksPath)
          val saved = sc.textFile(ranksPath).collect().toSet
          assert(saved == ranks.collect().map(_.toString).toSet, "Saved ranks must match the final RDD")
          println("Verified objectFile input and saveAsTextFile output in an isolated temporary directory.")
          println("All PairRDD PageRank checks passed.")
        } finally {
          ranks.unpersist()
          links.unpersist()
        }
      } finally {
        try {
          if (!fs.delete(root, true)) {
            throw new IllegalStateException(s"Could not remove temporary PageRank directory: $root")
          }
        } finally {
          fs.close()
        }
      }
    } finally {
      spark.stop()
    }
  }
}
