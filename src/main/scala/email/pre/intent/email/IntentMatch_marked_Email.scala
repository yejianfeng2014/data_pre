package email.pre.intent.email

import java.io.File
import java.util.Properties

import com.cybozu.labs.langdetect.DetectorFactory
import com.orderplus.UserNlp
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import pypal_pre.Language_dect

/**
  *
  * 查看当前的意图匹配能力水平，
  * 调出里面需要添加颜色的单词
  *
  */
object IntentMatch_marked_Email {

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
    val url_2 = "jdbc:mysql://127.0.0.1:3306/local_test_1"

    // 测试数据
    val mysqlDF: DataFrame = spark.read.jdbc(url, "bt_paypal_dispute_info_pre_first_message_buyer", properties)

    val mysqlDF_phrases: DataFrame = spark.read.jdbc(url_2, "bt_ai_intent_phrase", properties)

    // 意图短语

    val frame_intent_phrase: DataFrame = mysqlDF_phrases.select("intent_phrase")

    val phrases = scala.collection.mutable.Set[String]()

    frame_intent_phrase.collect().foreach { a =>
      val str: String = a.getString(0)
      phrases.+=(str)
      Unit
    }

    print(phrases.size)

    def getFirstBuyerString(str: String): Boolean = {
      if (phrases.contains(str)) true else false
    }

    val my_udf = udf(getFirstBuyerString _) //将自定义函数注册为udf
    val out = mysqlDF.withColumn("level_1", my_udf($"message_2")) //使用udf进行转换操作

    // 采用分句子的模式

    def getFirstBuyerString_2(str: String): Boolean = {
      val strings = UserNlp.splitSentence(str)
      val booleans = strings.map(a => phrases.contains(a))
      if (booleans.contains(true)) true else false
    }


    val my_udf_2 = udf(getFirstBuyerString_2 _) //将自定义函数注册为udf
    val out_2 = out.withColumn("level_2", my_udf_2($"message_2")) //使用udf进行转换操作
    // 连接词级别：

    def getFirstBuyerString_3(str: String): Boolean = {
      val strings = UserNlp.splitSentence(str)
      val booleans = strings.flatMap(a => UserNlp.SplitSentenceByConj(a)).map(a => phrases.contains(a.trim))
      if (booleans.contains(true)) true else false
    }

    val my_udf_3 = udf(getFirstBuyerString_3 _) //将自定义函数注册为udf


    val out_3 = out_2.withColumn("level_3", my_udf_3($"message_2")) //使用udf进行转换操作

    // 添加上连接词的分割，达到了7% 左右

    // 采用包含关系

    def getFirstBuyerString_4(str: String): Boolean = {
      val booleans = phrases.map(a => str.contains(a))
      if (booleans.contains(true)) true else false
    }

    val my_udf_4 = udf(getFirstBuyerString_4 _) //将自定义函数注册为udf
    val out_4 = out_3.withColumn("level_4", my_udf_4($"message_2")) //使用udf进行转换操作
    //    out_4.write.mode(saveMode = SaveMode.Overwrite).jdbc(url, "bt_paypal_dispute_info_pre_first_message_buyer_leve1", properties)


    // 做语言检测，把非英语的去掉


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

    val out_5 = out_4.withColumn("language", my_udf_language($"message_2")) //使用udf进行转换操作


    out_5.write.mode(SaveMode.Overwrite).jdbc(url, "bt_paypal_dispute_info_pre_first_message_buyer_mathc_level", properties)


    spark.stop()

  }


}
