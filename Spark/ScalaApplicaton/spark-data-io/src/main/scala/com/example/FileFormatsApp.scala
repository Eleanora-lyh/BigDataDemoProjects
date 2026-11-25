package com.example

import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.hadoop.io.compress.GzipCodec

import FileFormatSupport._

// 在项目目录运行：sbt "runMain com.example.FileFormatsApp"
// 先运行 PrepareFileInputsApp 准备文件；这里只演示读取和保存，不验证结果。
// 路径相对于运行目录；输出目录已存在时会报错，不会自动覆盖。
object FileFormatsApp {
  // ObjectFile 读写使用同一个对象类型，准备文件的入口也使用这个类。
  final case class Student(id: Int, name: String, score: Int)

  def main(args: Array[String]): Unit = {
    // 创建本地 Spark 应用，sc 用于 RDD 读写，spark 用于 DataFrame 读写。
    val spark = createSpark("Spark File Formats")
    try {
      val sc = spark.sparkContext

      // 1. 文本：每一行读成一个字符串，得到 RDD[String]。
      val input = sc.textFile("input/text/messages.txt")
      val outputFile = "output/text"
      // 保存的是目录，目录里的 part-* 文件存放文本内容。
      input.saveAsTextFile(outputFile)

      // 另一种读取方式：每个文件读成一条 (文件路径, 整个文件内容) 记录。
      val wholeFiles = sc.wholeTextFiles("input/text")

      // 2. gzip 文本：textFile 自动解压 .gz 文件，保存时指定压缩格式。
      val compressedText = sc.textFile("input/text-gzip")
      compressedText.saveAsTextFile("output/text-gzip", classOf[GzipCodec])

      // 3. JSON：每行一个 JSON 对象，Spark 自动推断字段名和类型。
      val json = spark.read.json("input/json/students.json")
      json.write.json("output/json")

      // 4. CSV：第一行作为列名，inferSchema 根据内容推断数字等字段类型。
      val csv = spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv("input/csv/students.csv")
      // 保存时也写入表头。
      csv.write.option("header", "true").csv("output/csv")

      // 5. Parquet：文件中已保存字段类型，直接读取和保存即可。
      val parquet = spark.read.parquet("input/parquet")
      parquet.write.parquet("output/parquet")

      // 6. ORC：同样自带字段类型，换成 orc 方法即可。
      val orc = spark.read.orc("input/orc")
      orc.write.orc("output/orc")

      // 7. SequenceFile：读取 Hadoop 键值文件，键是整数，值是文本。
      val sequence = sc.sequenceFile[IntWritable, Text]("input/sequence")
      // 将 Hadoop 类型转成 Scala 的 (Int, String)，再保存为键值文件。
      val pairs = sequence.map { case (id, name) => (id.get(), name.toString) }
      pairs.saveAsSequenceFile("output/sequence")

      // 8. ObjectFile：按准备文件时使用的 Student 类型读取，只读取可信文件。
      val students = sc.objectFile[Student]("input/object")
      students.saveAsObjectFile("output/object")
    } finally {
      // 读写结束或出现异常时，都关闭 Spark，释放资源。
      spark.stop()
    }
  }
}
