package com.example

import java.nio.file.{Path => LocalPath}

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}

private[example] object FileFormatSupport {
  // 支持的文件格式列表
  val formats: Seq[String] =
    Seq("text", "text-gzip", "json", "csv", "parquet", "orc", "sequence", "object")

  // 学生数据的 schema 定义
  val studentSchema: StructType = new StructType()
    .add("id", IntegerType, nullable = true)
    .add("name", StringType, nullable = true)
    .add("score", IntegerType, nullable = true)

  // 使用 file URI，避免 Windows 盘符被 Hadoop 误识别为 URI scheme。
  def filePath(directory: LocalPath, format: String): String =
    directory.resolve(format).toUri.toString
  // 创建本地 Spark 应用,保证 spark.stop() 一定会执行
  def createSpark(appName: String): SparkSession = {
    val spark = SparkSession.builder()
      .appName(appName)
      .master("local[2]")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.shuffle.partitions", "2")
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    spark
  }

  // RDD 保存 API 没有 overwrite 模式，仅删除当前格式的输出子目录。
  def overwriteRdd(spark: SparkSession, output: String)(save: String => Unit): Unit = {
    val target = new Path(output)
    val fs = target.getFileSystem(spark.sparkContext.hadoopConfiguration)
    if (fs.exists(target)) {
      println(s"Overwriting RDD output: $target")
      if (!fs.delete(target, true)) {
        throw new IllegalStateException(s"Failed to delete existing RDD output: $target")
      }
    }
    save(output)
  }
}
