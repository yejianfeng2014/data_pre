package pypal_pre

import java.io.{File, PrintWriter}
import java.util.Properties

import com.alibaba.fastjson.{JSON, JSONArray}
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

/**
  * 1 ，从原始表导出需要的字段（id, reason,dispute_life_cycle_stage,dispute_channel,messages,offer）
  * 2, 过滤掉messages  是null 的数据
  * 3，过滤掉messages 没有对话的数据
  * 3，过滤掉messages 里面只有一条对话的数据
  *
  */

object ReadDataFromMySQL {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("Spark SQL basic example")
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
    // 测试这个而函数

    //    val stri = "[{\"posted_by\":\"BUYER\",\"time_posted\":\"2018-09-04T10:55:08.000Z\"}]"
    //    val bool = isDialoge(stri)

    //    println(bool) // 这儿输出false 但是数据库里面是1 和0

    val my_udf = udf(isDialoge _) //将自定义函数注册为udf
    val out = value.withColumn("messages_1", my_udf($"messages")) //使用udf进行转换操作
    val frame_dialoge = out.filter(out("messages_1").===(true))

    println(frame_dialoge.count()) //47242

//    frame_dialoge.write.mode(saveMode = SaveMode.Overwrite).jdbc(url, "bt_paypal_dispute_info_pre_1", properties)

    val frame = frame_dialoge.select("messages")

//    frame.write.mode(SaveMode.Overwrite).json("./myjson.json")

    val writer = new PrintWriter(new File("learningScala.json"))


    frame.collect().foreach{
      a =>
       val string_1=  a.getString(0)
        writer.println(string_1)

    }
    spark.stop()


  }

}
