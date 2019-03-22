package emial.pre

import java.nio.file.{Files,  Paths}
import java.util.Properties

import org.apache.spark.sql.{DataFrame, SparkSession}


/**
  * 把处理好的数据放入本地文件中
  *
  * 数据格式，subject ##### 邮件内容
  *
  */
object SaveData_file {


  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .master("local[1]") // 这个需要单核跑，多核载入语言包出了问题
      .appName("seller language detect")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()

    import spark.implicits._

    val properties: Properties = new Properties()
    properties.setProperty("user", "demo")
    properties.setProperty("password", "123456")

    val url = "jdbc:mysql://127.0.0.1:3306/local_first_2"

    // todo fix this table name  use this  bt_email_inbox_content_parsed_html_language_fixed
    val mysqlDF: DataFrame = spark.read.jdbc(url, "bt_email_inbox_content_parsed_html_language_temp_1", properties)


    val frame = mysqlDF.select("subject","parsed_html")

    val value = frame.repartition(1)

//    value.write.option("delimiter","#").mode(SaveMode.Overwrite).csv("./ouput.csv")


//    import java.nio.file.Path
//    import java.nio.file.Paths
//    val path = Paths.get("./myfile.txt")
    val file: java.nio.file.Path =Paths.get("./myfile.txt") // target output file (i.e. 'out.csv')

    import scala.collection.JavaConversions._

    // write csv into temp directory which contains the additional spark output files
    // could use Files.createTempDirectory instead
    val tempDir = file.getParent.resolve(file.getFileName + "_tmp")
    frame.coalesce(1)
      .write.format("com.databricks.spark.csv")
      .option("header", "true")
      .save(tempDir.toAbsolutePath.toString)

    // find the actual csv file
    val tmpCsvFile = Files.walk(tempDir, 1).iterator().toSeq.find { p =>
      val fname = p.getFileName.toString
      fname.startsWith("part-00000") && fname.endsWith(".csv") && Files.isRegularFile(p)
    }.get

    // move to desired final path
    Files.move(tmpCsvFile, file)

    // delete temp directory
    Files.walk(tempDir)
      .sorted(java.util.Comparator.reverseOrder())
      .iterator().toSeq
      .foreach(Files.delete(_))







//    value.rdd.repartition(1).saveAsTextFile("./data/aas/AliMusic/submit_layout_data.csv")

//    value.repartition(1).saveAsTextFile("/data/aas/AliMusic/submit_layout_data.csv")

//    parkUtils.moveTempToFinalPat

    spark.stop()

  }


}
