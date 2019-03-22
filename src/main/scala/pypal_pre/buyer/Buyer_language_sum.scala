package pypal_pre.buyer

import java.io.File
import java.util.Properties

import com.alibaba.fastjson.{JSON, JSONArray}
import com.cybozu.labs.langdetect.DetectorFactory
import com.orderplus.Google_language_dec
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions.{explode, split, udf}
import pypal_pre.Language_dect

/**
  *  3:20 开始
  *
  */
object Buyer_language_sum {


  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .master("local[*]") // 这个需要单核跑，多核载入语言包出了问题
      .appName("buyer first dialogue")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()

    import spark.implicits._

    val properties: Properties = new Properties()
    properties.setProperty("user", "demo")
    properties.setProperty("password", "123456")

    val url = "jdbc:mysql://127.0.0.1:3306/local_first_2"
    val mysqlDF: DataFrame = spark.read.jdbc(url, "bt_paypal_dispute_info_buyer_all_says", properties)


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
    //
    //
    //    }
    //
    //
    //    val my_udf_2 = udf(getStringLanguage _) //将自定义函数注册为udf
    //
    //    val out = mysqlDF.withColumn("messages_1", my_udf_2($"buyer_message_1")) //使用udf进行转换操作


    //    out.write.mode(SaveMode.Overwrite).jdbc(url,"bt_paypal_dispute_info_buyer_all_says_dec",properties)


    // not en save one table

    //    val frame_not_en = out.filter(!out("messages_1").contains("en"))


    //    frame_not_en.write.mode(SaveMode.Overwrite).jdbc(url,"bt_paypal_dispute_info_buyer_all_says_not_en",properties)


    // 处理出错的检测结果，调用Google的服务，

    // 去掉检测失败数据

    // 去除检测中的中文简体和中文繁体，这个结果很准确。


    val frame_not_en_toDeal: DataFrame = spark.read.jdbc(url, "bt_paypal_dispute_info_buyer_all_says_not_en", properties)


    //    println(frame_not_en_toDeal.count()) // 19984

    val value = frame_not_en_toDeal.filter(frame_not_en_toDeal("messages_1") > " ")
    //    println(value.count()) // 19855
    val value_1 = value.filter(!frame_not_en_toDeal("messages_1").contains("zh-tw"))
    val value_2 = value_1.filter(!frame_not_en_toDeal("messages_1").contains("zh-cn"))
    //    val value = frame_not_en_toDeal.filter(frame_not_en_toDeal("messages_1") > " ")


    //    println(value_2.count()) // 19804


    // 这些需要检测


    // 检测lv 的语言


//    val frame_lv = value_2.filter(value_2("messages_1").contains("lv"))
    val frame_de = value_2.filter(value_2("messages_1").contains("de"))
//    val frame_lv = value_2.filter(value_2("messages_1").contains("lv"))
//    val frame_lv = value_2.filter(value_2("messages_1").contains("lv"))
//    val frame_lv = value_2.filter(value_2("messages_1").contains("lv"))
//    val frame_lv = value_2.filter(value_2("messages_1").contains("lv"))



    def getStringLanguage(str_my: String): String = {

      val res_result = Google_language_dec.getLanguageType(str_my)

//      Thread.s
      res_result
    }


    val my_udf_2 = udf(getStringLanguage _) //将自定义函数注册为udf

    val out = frame_de.withColumn("messages_2", my_udf_2($"buyer_message_1")) //使用udf进行转换操作

//    out.write.mode(SaveMode.Overwrite).jdbc(url,"bt_paypal_dispute_info_buyer_all_says_not_de",properties)


    // es

    val frame_es = value_2.filter(value_2("messages_1").contains("es"))
    val out_es = frame_es.withColumn("messages_2", my_udf_2($"buyer_message_1")) //使用udf进行转换操作

    out_es.write.mode(SaveMode.Append).jdbc(url,"bt_paypal_dispute_info_buyer_all_says_not_es",properties)


    spark.stop()

  }


}
