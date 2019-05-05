package email.pre.intent.email

import java.util.Properties

import com.orderplus.UserNlp
import org.apache.spark.sql.functions.{explode, split, udf}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}


/**
  *
  * 查看当前的意图匹配能力水平，
  * 1，直接匹配
  * 2，句子匹配
  * 3，短语匹配
  *
  *
  *
  */

// TODO:  把长文本变为句子
object LongText2SentencesEmail {

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

    // 测试数据
    val mysqlDF: DataFrame = spark.read.jdbc(url, "bt_paypal_dispute_info_pre_first_message_buyer_leve1", properties)


    def getFirstBuyerString_2(str: String): String = {

      val strings = UserNlp.splitSentence(str).map(a =>a.trim)

      strings.mkString("#")

    }


    val my_udf_2 = udf(getFirstBuyerString_2 _) //将自定义函数注册为udf


    val out_2 = mysqlDF.withColumn("message_3", my_udf_2($"message_2"))


    val frame_explode = out_2.withColumn("message_4", explode(split($"message_3", "[#]")))


//    val value = frame_explode.distinct()


//    value.write.mode(saveMode = SaveMode.Overwrite).jdbc(url, "bt_paypal_dispute_info_pre_first_message_buyer_leve2", properties)

    val frame = frame_explode.select("message_4").distinct()

    val frame_value = frame.filter($"message_4".isNotNull).filter($"message_4".contains(" "))

    def getFirstBuyerString_3(str: String): String = {


      if (str.startsWith("\"")){

        str.tail.trim
      }


      str.trim


    }


    val my_udf_3 = udf(getFirstBuyerString_3 _) //将自定义函数注册为udf


//    val out_2 = value.withColumn("message_5", my_udf_3($"message_4")) //使用udf进行转换操作

    val frame_5 = frame_value.withColumn("messege_5",my_udf_3($"message_4"))

    val value = frame_5.select("messege_5").distinct()

    value.write.mode(saveMode = SaveMode.Overwrite).jdbc(url, "bt_paypal_dispute_info_pre_first_message_buyer_leve5", properties)




    spark.stop()

  }


}
