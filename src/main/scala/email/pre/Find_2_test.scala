package email.pre

import java.util.Properties

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.types._


/**
  * 抽取两列，把字符串全部转数字看看效果，采用二分类看看效果
  *
  *
  */
object Find_2_test {



  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .master("local[1]")
      .config("spark.some.config.option", "some-value")
      .appName("buyer first dialogue")
      .getOrCreate()

    val properties: Properties = new Properties()
    properties.setProperty("user", "demo")
    properties.setProperty("password", "123456")

    //    val url = "jdbc:mysql://127.0.0.1:3306/email_webmail_one"
    val url = "jdbc:mysql://127.0.0.1:3306/local_first_2"

    // todo 修改读的邮件表的内容

    val mysqlDF: DataFrame = spark.read.jdbc(url, "bt_email_inbox_content_parsed_html_language_temp_10", properties)

    import  spark.implicits._

    val frame_2 = mysqlDF.filter($"labels".contains("Order tracking") or ($"labels".contains("Size doesn’t fit")))

//    frame_2.

    frame_2.write.mode(saveMode = SaveMode.Overwrite).jdbc(url,"bt_email_inbox_content_parsed_html_language_temp_11",properties)

    println(frame_2.count())






    spark.stop()

  }






}
