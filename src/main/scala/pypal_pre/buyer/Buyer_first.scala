package pypal_pre.buyer

import java.util.Properties

import com.alibaba.fastjson.{JSON, JSONArray}
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

/**
  * 统计所有买家，第一次发出来的内容 ，
  * 表字段  content  tatal   内容，总数
  *
  */

object Buyer_first {

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
          val nObject = array_2.getJSONObject(0)
          val str = nObject.getString("posted_by")
          val str_2 = nObject.getString("content")


         if (str_2.toLowerCase().trim =="clothing")  "111" else  ""


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

    val frame_string_first = frame_dialoge.withColumn("message_2", my_udf_2($"messages"))

    println(frame_string_first.count())


//    val frame_2 = frame_string_first.select("messages", "message_2")
//
//
//    //    frame_string_first.write.mode(saveMode = SaveMode.Append).jdbc(url, "bt_paypal_dispute_info_pre_first_buyer_temp", properties)
//    frame_2.write.mode(saveMode = SaveMode.Overwrite).jdbc(url, "bt_paypal_dispute_info_pre_first_buyer", properties)
//
//
//    val frame_message_buyer = frame_2.select("message_2")
//
//    val frame_total = frame_message_buyer.groupBy("message_2").count()

    frame_string_first.write.mode(saveMode = SaveMode.Overwrite).jdbc(url, "bt_paypal_dispute_info_pre_first_message_buyer_11", properties)

//    println("total first seller say :" + frame_total.count())

/*
    #####i haven’t even received a tracking or any information on this order .. wrote to the seller and haven’t received anything yet :(#####i’ve contacted the seller it says that it’s been delivered.. i live in a 8 apt complex and when to see indivualy all of them and my package is still no were to find .. i’ve paid for this item so i would appreciate a full refund
    regards*/


    spark.stop()

  }

}
