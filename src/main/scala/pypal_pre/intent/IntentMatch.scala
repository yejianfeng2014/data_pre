package pypal_pre.intent

import java.util.Properties

import com.orderplus.UserNlp
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions.udf


/**
  *
  * 查看当前的意图匹配能力水平，
  * 1，直接匹配
  * 2，句子匹配
  * 3，短语匹配
  *
  *
  * 测试数据量：45105
  *
  */
object IntentMatch {

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

    out_3.write.mode(saveMode = SaveMode.Overwrite).jdbc(url, "bt_paypal_dispute_info_pre_first_message_buyer_leve1", properties)


    // 添加上连接词的分割，达到了7% 左右

    spark.stop()

  }


}
