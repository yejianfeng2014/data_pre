package pypal_pre.buyer

import java.util.Properties

import com.alibaba.fastjson.{JSON, JSONArray}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions.udf

import scala.collection.mutable.ArrayBuffer

/**
  *  统计buyer 总共发送量，构成对话的前提下,这个后期没有单独计算的必要，
  *  在所有buyer 的对话count 就可以出来，但是这个统计比那个处理的过程速度要快
  */

object Buyer_total_says {


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
    //    println(mysqlDF.count()) // 88359
    val value = mysqlDF.filter(mysqlDF("messages").isNotNull)
    //    println(value.count()) //83749
    def isDialoge(str: String): Boolean = {
      try {
        val array_2: JSONArray = JSON.parseArray(str)
        if (array_2.size() > 1) true else false
      } catch {
        case e:Exception => false
      }
    }

    def getFirstBuyerString(str: String): Int = {
      // 添加try 主要是为了 防止初心json 解析失败的情况
      try {
        val array_2: JSONArray = JSON.parseArray(str)

        val myArray = ArrayBuffer[Int]()

          for (i <- 0 until (array_2.size())) {
            val nObject = array_2.getJSONObject(i)

            val str_my = nObject.getString("posted_by")

            if (str_my == "BUYER") {
              myArray += 1
            }
          }

        val i = myArray.sum
        i

      } catch {
        case e:Exception => 0
      }
    }


    val my_udf = udf(isDialoge _)
    val my_udf_2 = udf(getFirstBuyerString _)
    val out = value.withColumn("messages_1", my_udf($"messages"))
    val frame_dialoge = out.filter(out("messages_1") === (true))

    val frame_string_first = frame_dialoge.withColumn("message_2", my_udf_2($"messages"))


    frame_string_first.createOrReplaceTempView("message_all")

    val sql = "SELECT SUM(message_2) as total FROM message_all"

    val frame_total = spark.sql(sql)

    val l = frame_total.select("total").collectAsList().get(0).getLong(0)


    println(l) // 94946

    spark.stop()

  }



}
