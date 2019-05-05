package email.pre.intent.email

import java.io.File
import java.util.Properties

import com.cybozu.labs.langdetect.DetectorFactory
import com.orderplus.UserNlp
import com.vdurmont.emoji.EmojiParser
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions.udf
import pypal_pre.Language_dect

/**
  *
  * 查看当前的意图匹配能力水平，
  * 1，直接匹配
  * 2，句子匹配
  * 3，短语匹配
  * 4，包含匹配 20% 左右
  * 5，单词包含匹配
  *
  * 测试数据量：45105
  *
  * 过滤完英语的数量：32740
  *
  * 最终匹配结果28.65%
  *
  */

// TODO:  检测10w 条数据生成了14万的结果，查看哪儿出了问题
// TODO:  把识别和语言检测分开,语言检测不能多核跑
object IntentMatchEmail {

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

    //    val url = "jdbc:mysql://172.21.1.91:3306/test?useUnicode=yes&characterEncoding=UTF-8"

    val url_2 = "jdbc:mysql://127.0.0.1:3306/local_data_tongji"

    // 测试数据
    val mysqlDF: DataFrame = spark.read.jdbc(url_2, "bt_ai_email", properties)

    println(mysqlDF.count()) // 10,000


    // 过滤掉不符合规则的数据

    //     val frame_filtered = mysqlDF.filter($"text_plain" >" " && $"text_html" >" " )


    //    println(frame_filtered.count()) // 60602


    val frame_selected = mysqlDF.select("id", "email_address", "subject", "text_plain", "add_time")


    val value = frame_selected.filter($"text_plain" > " ")

    println("过滤完空字符串后的数据： " + value.count())

    // 把表情符号去掉

    def getFirstBString(str: String): String = {

      val string = EmojiParser.removeAllEmojis(str)

      //      val str_new = str.replaceAll("[\\ud800\\udc00-\\udbff\\udfff\\ud800-\\udfff]", " ")


      //      val str_1 = str_new.toLowerCase()

      string.toLowerCase()

      //      val str_re = str_new.replaceAll("[\\x{10000}-\\x{10FFFF}]", "")
      //
      //
      //     val str_2 = str_re.replaceAll("\\xFFFD", "")

    }

    val my_udf_str = udf(getFirstBString _) //将自定义函数注册为udf


    val out_replaced = value.withColumn("text_plain_1", my_udf_str($"text_plain")) //使用udf进行转换操作


    val out_replaced_new = out_replaced.select("id", "email_address", "subject", "text_plain_1", "add_time")


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
    val out = out_replaced_new.withColumn("level_1", my_udf($"text_plain_1")) //使用udf进行转换操作

    // 采用分句子的模式

    def getFirstBuyerString_2(str: String): Boolean = {
      val strings = UserNlp.splitSentence(str)
      val booleans = strings.map(a => phrases.contains(a))
      if (booleans.contains(true)) true else false
    }


    val my_udf_2 = udf(getFirstBuyerString_2 _) //将自定义函数注册为udf
    val out_2 = out.withColumn("level_2", my_udf_2($"text_plain_1")) //使用udf进行转换操作
    // 连接词级别：

    def getFirstBuyerString_3(str: String): Boolean = {
      val strings = UserNlp.splitSentence(str)
      val booleans = strings.flatMap(a => UserNlp.SplitSentenceByConj(a)).map(a => phrases.contains(a.trim))
      if (booleans.contains(true)) true else false
    }

    val my_udf_3 = udf(getFirstBuyerString_3 _) //将自定义函数注册为udf


    val out_3 = out_2.withColumn("level_3", my_udf_3($"text_plain_1")) //使用udf进行转换操作

    // 添加上连接词的分割，达到了7% 左右

    // 采用包含关系

    def getFirstBuyerString_4(str: String): Boolean = {
      val booleans = phrases.map(a => str.contains(a))
      if (booleans.contains(true)) true else false
    }

    val my_udf_4 = udf(getFirstBuyerString_4 _) //将自定义函数注册为udf
    val out_4 = out_3.withColumn("level_4", my_udf_4($"text_plain_1")) //使用udf进行转换操作
    out_4.write.mode(saveMode = SaveMode.Overwrite).jdbc(url_2, "tmp_1", properties)


    spark.stop()

  }


}
