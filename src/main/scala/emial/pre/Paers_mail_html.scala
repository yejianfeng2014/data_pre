package emial.pre

import java.util.Properties
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.jsoup.Jsoup
import org.jsoup.nodes.Document
import org.jsoup.select.Elements

/**
  *
  * 实现解析邮件中的html 中邮件正文的功能，可以解析的html 格式参见
  *  test/scala/my_spark 中的faf.html 的结构
  *
  *
  */

object Paers_mail_html {


  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("buyer first dialogue")
      .config("spark.some.config.option", "some-value")
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

    import spark.implicits

    val my_udf = udf(pareseHtml _) //将自定义函数注册为udf
    val out = mysqlDF.withColumn("parsed_html", my_udf(mysqlDF("text_html"))) //使用udf进行转换操作

    val frame_parsed_html_content = out.select("subject","parsed_html")

    frame_parsed_html_content.write.mode(SaveMode.Overwrite).jdbc(url, "bt_email_inbox_content_parsed_html ", properties)

    spark.stop()


  }


}
