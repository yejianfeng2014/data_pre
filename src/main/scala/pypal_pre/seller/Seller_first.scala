package pypal_pre.seller

import java.util.Properties

import com.alibaba.fastjson.{JSON, JSONArray}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions.udf


/**
  * 获取第一个seller 发送的消息
  *
  */
object Seller_first {


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


    val mysqlDF: DataFrame = spark.read.jdbc(url, "bt_paypal_dispute_info", properties)


    val value = mysqlDF.filter(mysqlDF("messages").isNotNull)


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
          val nObject = array_2.getJSONObject(0)
          val str = nObject.getString("posted_by")
          val str_2 = nObject.getString("content")


          if (str == "SELLER") {
            str_2.trim
          }
          else {
            val nObject = array_2.getJSONObject(1)
            val str_new_1 = nObject.getString("posted_by")
            val str_new_2 = nObject.getString("content")


            if (str_new_1 == "SELLER") {
              str_new_2.trim
            } else {
              ""
            }
          }
        } else {
          ""
        }
      } catch {
        case e:Exception => ""
      }
    }


    val my_udf = udf(isDialoge _) //将自定义函数注册为udf
    val my_udf_2 = udf(getFirstBuyerString _) //将自定义函数注册为udf
    val out = value.withColumn("messages_1", my_udf($"messages")) //使用udf进行转换操作
    val frame_dialoge = out.filter(out("messages_1") === (true))

    val frame_string_first = frame_dialoge.withColumn("seller_message", my_udf_2($"messages"))

    println(frame_string_first.count())


    val frame_2 = frame_string_first.select("messages", "seller_message")


    //    frame_string_first.write.mode(saveMode = SaveMode.Append).jdbc(url, "bt_paypal_dispute_info_pre_first_buyer_temp", properties)
    frame_2.write.mode(saveMode = SaveMode.Overwrite).jdbc(url, "bt_paypal_dispute_info_pre_first_seller", properties)


    val frame_message_buyer = frame_2.select("seller_message")

    val frame_total = frame_message_buyer.groupBy("seller_message").count()

    frame_total.write.mode(saveMode = SaveMode.Overwrite).jdbc(url, "bt_paypal_dispute_info_pre_first_seller_sum", properties)


    spark.stop()

  }

}
