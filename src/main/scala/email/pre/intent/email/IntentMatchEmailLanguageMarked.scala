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
object IntentMatchEmailLanguageMarked {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("buyer first dialogue")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()

    import spark.implicits._

    val properties: Properties = new Properties()
    properties.setProperty("user", "demo")
    properties.setProperty("password", "123456")

    val url = "jdbc:mysql://127.0.0.1:3306/local_data_tongji"
    //    val url_2 = "jdbc:mysql://127.0.0.1:3306/local_test_1"

    // 测试数据
    val mysqlDF: DataFrame = spark.read.jdbc(url, "match_email_result", properties)

    // 过滤匹配到的数据

    val frame_matched = mysqlDF.filter($"level_4".===(1) or ($"level_3".===(1)) or ($"level_2".===(1)) or ($"level_1").===(1))

    //    val frame_matched = mysqlDF.filter($"level_1">0 and($"level_2">0) and($"level_3">0) and($"level_4")>0)

    println("matched frame size : " + frame_matched.count())

//    frame_matched.write.mode(SaveMode.Overwrite).jdbc(url, "tmp_2", properties)


    val mysqlDF_phrases: DataFrame = spark.read.jdbc(url, "bt_ai_intent_phrase", properties)
    //
    //    // 意图短语

    val frame_intent_phrase: DataFrame = mysqlDF_phrases.select("intent_phrase")

    val phrases = scala.collection.mutable.Set[String]()

    frame_intent_phrase.collect().foreach { a =>
      val str: String = a.getString(0)
      phrases.+=(str)
      Unit
    }

    print(phrases.size)

    //
    def getFirstBuyerString(str: String): String = {
      val result = if (phrases.contains(str)) true else false

      if (result) {
        str
      } else {
        ""
      }
    }

    val my_udf = udf(getFirstBuyerString _) //将自定义函数注册为udf

    val out = frame_matched.withColumn("level_1_data", my_udf($"text_plain_1")) //使用udf进行转换操作


    // 采用分句子的模式

    def getFirstBuyerString_2(str: String): String = {
      val strings = UserNlp.splitSentence(str)
      val booleans = strings.map(a => phrases.contains(a))
      val result = if (booleans.contains(true)) true else false

      if (result) {
        val strings_new = strings.filter(a => phrases.contains(a))
        strings_new.mkString("_")
      } else {
      }
      ""
    }


    val my_udf_2 = udf(getFirstBuyerString_2 _) //将自定义函数注册为udf
    val out_2 = out.withColumn("level_2_data", my_udf_2($"text_plain_1")) //使用udf进行转换操作
    // 连接词级别：

    def getFirstBuyerString_3(str: String): String = {
      val strings = UserNlp.splitSentence(str)
      val booleans = strings.flatMap(a => UserNlp.SplitSentenceByConj(a)).map(a => phrases.contains(a.trim))
      val result = if (booleans.contains(true)) true else false


      if (result) {
        val strings_new = strings.filter(a => phrases.contains(a))
        strings_new.mkString("_")
      } else {
        ""
      }


    }

    val my_udf_3 = udf(getFirstBuyerString_3 _) //将自定义函数注册为udf


    val out_3 = out_2.withColumn("level_3_data", my_udf_3($"text_plain_1")) //使用udf进行转换操作

    // 添加上连接词的分割，达到了7% 左右

    // 采用包含关系

    def getFirstBuyerString_4(str: String): String = {
      val booleans = phrases.map(a => str.contains(a))
      val result = if (booleans.contains(true)) true else false


      if (result) {
        val strings_new = phrases.filter(a => str.contains(a))
        strings_new.mkString("_")
      } else {
        ""
      }


    }

    val my_udf_4 = udf(getFirstBuyerString_4 _) //将自定义函数注册为udf
    val out_4 = out_3.withColumn("level_4_data", my_udf_4($"text_plain_1")) //使用udf进行转换操作


    out_4.write.mode(SaveMode.Overwrite).jdbc(url, "match_email_result_marked", properties)


    spark.stop()

  }


}
