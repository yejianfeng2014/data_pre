package pypal_pre

import java.io.File
import java.util.Properties

import com.alibaba.fastjson.{JSON, JSONArray}
import com.cybozu.labs.langdetect.DetectorFactory
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions.udf

/**
  * 构建对话，结构对
  */

object Buyer_Seller_dialogues {


  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .master("local[1]")
      .appName("buyer first dialogue")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()

    import spark.implicits._

    val properties: Properties = new Properties()
    properties.setProperty("user", "demo")
    properties.setProperty("password", "123456")

    val url = "jdbc:mysql://127.0.0.1:3306/local_first_2"
    val mysqlDF: DataFrame = spark.read.jdbc(url, "bt_paypal_dispute_info", properties)

    val value = mysqlDF.filter(mysqlDF("messages").isNotNull)

    def isDialoge(str: String): Boolean = {
      try {
        val array_2: JSONArray = JSON.parseArray(str)
        if (array_2.size() > 1) true else false
      } catch {
        case e: Exception => false
      }
    }


    val uri = Language_dect.getClass.getResource("/profiles").toURI()

    DetectorFactory.loadProfile(new File(uri))


    def getFirstBuyerString(str: String): String = {
      try {
        val array_2: JSONArray = JSON.parseArray(str)

        if (array_2.size() > 1) {
          val nObject = array_2.getJSONObject(0)

          //          array_2.toArray()
          val str = nObject.getString("posted_by")
          val str_2 = nObject.getString("content")

          val str_re = str_2.replaceAll("[\\x{10000}-\\x{10FFFF}]", "")

          if (str == "BUYER") {

            val detector = DetectorFactory.create()

            detector.append(str_re)

            val str_lan = detector.detect()

            return str_lan

          }
          ""
        } else {

          " "
        }
      } catch {
        case e: Exception => ""
      }
    }


    val my_udf = udf(isDialoge _) //将自定义函数注册为udf
    val my_udf_2 = udf(getFirstBuyerString _) //将自定义函数注册为udf
    val out = value.withColumn("messages_1", my_udf($"messages")) //使用udf进行转换操作
    val frame_dialoge = out.filter(out("messages_1") === (true))


    val frame_string_first = frame_dialoge.withColumn("message_2", my_udf_2($"messages"))


    val frame_2 = frame_string_first.select("messages", "message_2")


    frame_2.write.mode(saveMode = SaveMode.Overwrite).jdbc(url, "bt_paypal_dispute_info_pre_all_says", properties)


    spark.stop()

  }

}
