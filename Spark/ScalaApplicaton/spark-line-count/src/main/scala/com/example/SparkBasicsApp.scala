package com.example

import org.apache.spark.sql.{ SparkSession}


object SparkBasicsApp {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Scala Spark Playground")
      .master("local[2]")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.shuffle.partitions", "2")
      .getOrCreate()

    val sc = spark.sparkContext
    sc.setLogLevel("WARN")
    try {
        
        // val rdd = sc.parallelize(Seq(1, 2, 3, 4, 5, 6), numSlices = 3)   // numSlices 指定分区数
        // println("NumPartitions: " + rdd.getNumPartitions)  // 3
        // println("NumDetails: " + rdd.glom().collect().map(_.toList).toList)     // [[1, 2], [3, 4], [5, 6]]，可看到每个分区的内容
    
        val rdd_lines = sc.textFile("data/input.txt")
        val spark_rdd = rdd_lines.filter(line => line.contains("Spark"))
        spark_rdd.foreach(println)
    } finally {
      spark.stop()
    }
  }
}
