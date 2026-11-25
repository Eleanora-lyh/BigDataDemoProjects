package com.example

import java.nio.file.Paths

import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.compress.GzipCodec

import FileFormatSupport._
import FileFormatsApp.Student

// 首次运行：sbt "runMain com.example.PrepareFileInputsApp"
object PrepareFileInputsApp {
  def main(args: Array[String]): Unit = {
    // 参数校验：只允许 0 或 1 个参数。如果没传参数 → 默认 "input"
    require(args.length <= 1, "Usage: PrepareFileInputsApp [input-directory]")
    // 获取输入目录的绝对路径并规范化
    val input = Paths.get(args.headOption.getOrElse("input")).toAbsolutePath.normalize()
    // 把 filePath(input, format) 这个较长的调用，简化成 path(format)
    def path(format: String): String = filePath(input, format)
    // 创建本地 Spark 应用
    val spark = createSpark("Prepare Spark File Inputs")
    try {
      val sc = spark.sparkContext
      //启用 Encoder 隐式转换, 后面 .as[Student] 依赖它
      import spark.implicits._
      // 检查必需的输入样例是否存在,不存在就直接抛异常，终止程序
      Seq("text", "json", "csv").foreach { format =>
        val source = new Path(path(format))
        require(
          source.getFileSystem(sc.hadoopConfiguration).exists(source),
          s"Missing source samples: $source"
        )
      }

      // 只生成缺失的输入，不覆盖手动修改过的样例。
      def generate(format: String)(save: String => Unit): Unit = {
        val target = new Path(path(format))// 目标路径（Hadoop 的 Path 对象）
        val fs = target.getFileSystem(sc.hadoopConfiguration)// 取得该路径所在的文件系统（HDFS/本地/ADLS 等）
        if (fs.exists(target)) {// 目录已存在：必须有 _SUCCESS 标记，说明上次是完整生成的，完整就跳过，不重复生成
          require(
            fs.exists(new Path(target, "_SUCCESS")),
            s"Incomplete generated input: $target. Move it aside before retrying."
          )
          println(s"Keeping existing input: $target")
        } else {// 不存在才调用传进来的函数去生成
          println(s"Generating input: $target")
          save(path(format))
        }
      }
      
      val students = spark.read.schema(studentSchema) //指定 schema（不依赖推断）
        .option("mode", "FAILFAST") // FAILFAST：遇到坏数据直接失败
        .json(path("json")).as[Student] //.as[Student]：转成 Dataset[Student]

      // 从 text 读取，保存为 Gzip 压缩的 Text 文件
      generate("text-gzip") { target =>
        sc.textFile(path("text")).saveAsTextFile(target, classOf[GzipCodec])
      }
      // Dataset[Student] parquet 存储
      generate("parquet")(target => students.write.mode("errorifexists").parquet(target))
      // Dataset[Student] orc 存储
      generate("orc")(target => students.write.mode("errorifexists").orc(target))
      // Dataset[Student] sequence 存储
      generate("sequence") { target =>
        students.rdd.map(student => student.id -> student.name).saveAsSequenceFile(target)
      }
      // Dataset[Student] object 存储
      generate("object")(target => students.rdd.saveAsObjectFile(target))

      println(s"Input samples ready: $input")
    } finally {
      spark.stop()
    }
  }
}
