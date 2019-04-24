package pypal_pre.buyer

import java.io.File
import java.util.Properties

import com.cybozu.labs.langdetect.DetectorFactory
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import pypal_pre.Language_dect

/**
  * 统计所有买家，第一次发出来的内容,过滤出来英文的内容 ，
  *
  *
  */

object Buyer_first_en {

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

    val url = "jdbc:mysql://127.0.0.1:3306/local_first_2"


    //todo:3、读取mysql中的数据
    val mysqlDF: DataFrame = spark.read.jdbc(url, "bt_paypal_dispute_info_pre_first_message_buyer", properties)
    //todo:4、显示mysql中表的数据



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

    import spark.implicits._

    val my_udf_2 = udf(getStringLanguage _) //将自定义函数注册为udf

    val frame_parsed_html_content_result = mysqlDF.withColumn("language_dec", my_udf_2($"message_2")) //使用udf进行转换操作

    val value = frame_parsed_html_content_result.filter($"language_dec".contains("en"))


    value.write.mode(saveMode = SaveMode.Overwrite).jdbc(url, "bt_paypal_dispute_info_pre_first_message_buyer_en", properties)


    spark.stop()

  }

}
