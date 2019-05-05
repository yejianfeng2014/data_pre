package email.pre.intent.email

import java.io.File
import java.util.Properties

import com.cybozu.labs.langdetect.DetectorFactory
import com.orderplus.UserNlp
import com.vdurmont.emoji.EmojiParser
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import pypal_pre.Language_dect

/**
  *
  * 查看当前的意图匹配能力水平，
  * 1,添加语言检测结果
  */

// TODO:  把识别和语言检测分开,语言检测不能多核跑
// 采用本地跑，跑完把结果移动过去。10w级别的数据移动很快
object IntentMatchEmailLanguage {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .master("local[1]")
      .appName("language_dec")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()

    import spark.implicits._

    val properties: Properties = new Properties()
    properties.setProperty("user", "demo")
    properties.setProperty("password", "123456")

    val url = "jdbc:mysql://127.0.0.1:3306/local_data_tongji"

//    val url_2 = "jdbc:mysql://127.0.0.1:3306/local_test_1"

    val mysqlDF: DataFrame = spark.read.jdbc(url, "tmp_1", properties)

    val uri = Language_dect.getClass.getResource("/profiles").toURI()

    DetectorFactory.loadProfile(new File(uri))

    def getStringLanguage(str_my: String): String = {

      val detector = DetectorFactory.create()

      detector.append(str_my)
      try {
        val str = detector.detect()

        str
      } catch {
        case e: Exception => ""
      }
    }


    val my_udf_language = udf(getStringLanguage _) //将自定义函数注册为udf

    val out_5 = mysqlDF.withColumn("language", my_udf_language($"text_plain_1")) //使用udf进行转换操作


    out_5.write.mode(SaveMode.Append).jdbc(url, "match_email_result", properties)


    spark.stop()

  }


}
