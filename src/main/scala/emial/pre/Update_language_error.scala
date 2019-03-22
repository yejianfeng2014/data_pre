package emial.pre

import java.util.Properties

import com.orderplus.Google_language_dec
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions.udf


/**
  * 1500 个处理时间是：大概15分钟
  *
  */
object Update_language_error {


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
    val mysqlDF: DataFrame = spark.read.jdbc(url, "bt_email_inbox_content_parsed_html_language", properties)


    def getStringLanguage(str_my: String): String = {

      val res_result = Google_language_dec.getLanguageType(str_my)
      res_result
    }

    val frame_en = mysqlDF.filter(mysqlDF("language_dec").contains("en"))


    // 所有不是英语的的文章
    val frame_en_not = mysqlDF.filter(!mysqlDF("language_dec").contains("en"))


    val my_udf_2 = udf(getStringLanguage _) //将自定义函数注册为udf

    val out = frame_en_not.withColumn("language_dec_1", my_udf_2($"parsed_html")) //使用udf进行转换操作

    //    out.write.mode(saveMode =SaveMode.Overwrite).jdbc(url,"bt_email_inbox_content_parsed_html_language_temp",properties)


    // todo 英文对的和错的合并，生产环境不建议这么处理，建议采用把检测不是英文的的在以前已经检测好的结果里面进行一遍搜索处理，
    //  采用访问缓存数据库的模式，肯定比网络访问效率高很多

    val updated_end = out.filter($"language_dec_1".contains("en"))

    val frame_selected = updated_end.select("subject", "parsed_html", "language_dec_1")
    val frame_union_languages = frame_en.union(frame_selected)

    val frame = frame_union_languages.select("subject","parsed_html")
    frame.write.mode(SaveMode.Overwrite).jdbc(url, "bt_email_inbox_content_parsed_html_language_fixed", properties)

    spark.stop()

  }

}
