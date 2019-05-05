package email.pre

import java.io.File
import java.util.Properties

import com.cybozu.labs.langdetect.DetectorFactory
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.jsoup.Jsoup
import org.jsoup.nodes.Document
import org.jsoup.select.Elements
import pypal_pre.Language_dect

/**
  *
  * 实现解析邮件中的html 中邮件正文的功能，可以解析的html 格式参见
  *  test/scala/my_spark 中的faf.html 的结构
  */

object Paers_mail_html {


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

    val mysqlDF: DataFrame = spark.read.jdbc(url, "sqlresult_2303603", properties)

    def pareseHtml(str: String): String = {
      val doc: Document = Jsoup.parse(str)
      try {
        val element = doc.select("div > table").get(1)
        val elements: Elements = element.getElementsByTag("div").get(0).children()
        val str_with_content = elements.last().text()
        if  (str_with_content.startsWith("content: ")) str_with_content.substring(9,str_with_content.length-1) else  str_with_content


      } catch {
        // 结构不符合标准结构，报错的暂时未做处理，有时间查看为什么报错了。返回的我用字符串error 标记
        case e: Exception => "error"
      }
    }



    val my_udf = udf(pareseHtml _) //将自定义函数注册为udf
    val out = mysqlDF.withColumn("parsed_html", my_udf(mysqlDF("text_html"))) //使用udf进行转换操作

//    val frame_parsed_html_content = out.select("subject","parsed_html")

    out.write.mode(SaveMode.Overwrite).jdbc(url, "bt_email_inbox_content_parsed_html ", properties)


//    // 语言检测：
//
//
//    val uri = Language_dect.getClass.getResource("/profiles").toURI()
//
//    DetectorFactory.loadProfile(new File(uri))
//
//    def getStringLanguage(str_my: String): String = {
//
//      val detector = DetectorFactory.create()
//
//      detector.append(str_my)
//      try {
//        val str = detector.detect()
//
//        str
//      } catch {
//        case e: Exception => ""
//      }
//    }
//
//    import spark.implicits._
//
//    val my_udf_2 = udf(getStringLanguage _) //将自定义函数注册为udf
//
//    val frame_parsed_html_content_result = frame_parsed_html_content.withColumn("language_dec", my_udf_2($"parsed_html")) //使用udf进行转换操作
//
//
//    frame_parsed_html_content_result.write.mode(SaveMode.Overwrite).jdbc(url, "bt_email_inbox_content_parsed_html_language", properties)
//
//
//


    spark.stop()




  }


}
