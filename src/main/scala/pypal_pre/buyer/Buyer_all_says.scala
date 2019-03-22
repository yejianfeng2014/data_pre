package pypal_pre.buyer

import java.util.Properties

import com.alibaba.fastjson.{JSON, JSONArray}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions._


case class Seller(poster: String, content: String, date: String)

object Buyer_all_says {


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

    val url = "jdbc:mysql://127.0.0.1:3306/local_first_2"


    //todo:3、读取mysql中的数据
    val mysqlDF: DataFrame = spark.read.jdbc(url, "bt_paypal_dispute_info", properties)
    //todo:4、显示mysql中表的数据


    //    println(mysqlDF.count()) // 88359

    val value = mysqlDF.filter(mysqlDF("messages").isNotNull)

    //    println(value.count()) //83749


    // 传入一个字符串，返回一个ture or false 的值

    def isDialoge(str: String): Boolean = {

      // 添加try 主要是为了 防止初心json 解析失败的情况
      try {
        val array_2: JSONArray = JSON.parseArray(str)
        if (array_2.size() > 1) true else false
      } catch {
        case e:Exception => false
      }
    }

    def getFirstBuyerString(str: String): String = {

      // 添加try 主要是为了 防止初心json 解析失败的情况
      try {
        val array_2: JSONArray = JSON.parseArray(str)


        if (array_2.size() > 1) {

          var total_str = ""

          for (i <- 0 until (array_2.size())) {
            val nObject = array_2.getJSONObject(i)

            val str = nObject.getString("posted_by")
            val str_2 = nObject.getString("content")

            if (str == "BUYER") {
              total_str = str_2.trim + "#####" + total_str
            } else {
              ""
            }
          }
          // 解决字符编码的问题
          val str_re = total_str.replaceAll("[\\x{10000}-\\x{10FFFF}]", "")

          val str = str_re.toLowerCase()

          str

        }
        else {

          ""
        }
      }

      catch {
        case e:Exception => ""
      }
    }


    val my_udf = udf(isDialoge _) //将自定义函数注册为udf
    val my_udf_2 = udf(getFirstBuyerString _) //将自定义函数注册为udf
    val out = value.withColumn("messages_1", my_udf($"messages")) //使用udf进行转换操作
    val frame_dialoge = out.filter(out("messages_1") === (true))

    val frame_string_first = frame_dialoge.withColumn("buyer_message", my_udf_2($"messages"))

    //    println(frame_string_first.count())


    val frame_2 = frame_string_first.select("messages", "buyer_message")


    val frame_explode = frame_2.withColumn("buyer_message_1", explode(split($"buyer_message", "[#####]")))


    val frame_filtered = frame_explode.filter(frame_explode("buyer_message_1") > " ")


    val frame_all_buyer_messages = frame_filtered.select("buyer_message_1")

    val frame_all_buyer_messages_count = frame_all_buyer_messages.groupBy("buyer_message_1").count()


    frame_all_buyer_messages_count.write.mode(saveMode = SaveMode.Overwrite).jdbc(url, "bt_paypal_dispute_info_buyer_all_says", properties)



    spark.stop()

  }

}
