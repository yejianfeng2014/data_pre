package emial.pre

import java.io.File
import java.util.Properties
import com.cybozu.labs.langdetect.DetectorFactory
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions.udf
import org.jsoup.Jsoup
import org.jsoup.nodes.Document
import org.jsoup.select.Elements
import pypal_pre.Language_dect


/**
  *
  * 找出最长的序列
  *
  *
  */
object Sum_long_string {



  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .master("local[*]")
      .config("spark.some.config.option", "some-value")
      .appName("buyer first dialogue")
      .getOrCreate()

    val properties: Properties = new Properties()
    properties.setProperty("user", "demo")
    properties.setProperty("password", "123456")

    //    val url = "jdbc:mysql://127.0.0.1:3306/email_webmail_one"
    val url = "jdbc:mysql://127.0.0.1:3306/local_first_2"

    // todo 修改读的邮件表的内容

    val mysqlDF: DataFrame = spark.read.jdbc(url, "bt_email_inbox_content_parsed_html_language_temp_1", properties)

    def pareseHtml(str: String): Int = {

      str.length
    }



    val my_udf = udf(pareseHtml _) //将自定义函数注册为udf

    val out = mysqlDF.withColumn("data_length", my_udf(mysqlDF("parsed_html"))) //使用udf进行转换操作


    out.write.mode(SaveMode.Overwrite).jdbc(url,"bt_email_inbox_content_parsed_html_language_temp_2",properties)


    spark.stop()




  }





}
