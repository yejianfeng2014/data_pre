package com.data.other

import java.util.Properties

import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import org.apache.spark.sql.functions._
/**
  *  回复模板合并
  *
  *
  */
object ResponseDataUnion {



  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .master("local[1]")
      .config("spark.some.config.option", "some-value")
      .appName("buyer first dialogue")
      .getOrCreate()

    val properties: Properties = new Properties()
    properties.setProperty("user", "demo")
    properties.setProperty("password", "123456")

    //    val url = "jdbc:mysql://127.0.0.1:3306/email_webmail_one"
    val url = "jdbc:mysql://127.0.0.1:3306/local_first_2"

    // todo 修改读的邮件表的内容

    val mysqlDF_1: DataFrame = spark.read.jdbc(url, "email_sell_response_beggin", properties)
    val mysqlDF_2: DataFrame = spark.read.jdbc(url, "email_sell_response_end", properties)
    val mysqlDF_3: DataFrame = spark.read.jdbc(url, "email_sell_response_mid", properties)
    val mysqlDF_4: DataFrame = spark.read.jdbc(url, "email_sell_response_others", properties)




    def pareseHtml(str: String): String = {
      "begin"
    }

    val my_udf = udf(pareseHtml _) //将自定义函数注册为udf

    val frame_1 = mysqlDF_1.withColumn("response_time",my_udf(mysqlDF_1("id")))




    def pareseHtml_2(str: String): String = {
      "end"
    }

    val my_udf_2 = udf(pareseHtml_2 _) //将自定义函数注册为udf

    val frame_2 = mysqlDF_2.withColumn("response_time",my_udf_2(mysqlDF_2("id")))



    def pareseHtml_3(str: String): String = {
      "mid"
    }

    val my_udf_3 = udf(pareseHtml_3 _) //将自定义函数注册为udf

    val frame_3 = mysqlDF_3.withColumn("response_time",my_udf_3(mysqlDF_3("id")))


    def pareseHtml_4(str: String): String = {
      "others"
    }

    val my_udf_4 = udf(pareseHtml_4 _) //将自定义函数注册为udf

    val frame_4 = mysqlDF_4.withColumn("response_time",my_udf_4(mysqlDF_4("id")))



     val frame_all = frame_1.union(frame_2).union(frame_3).union(frame_4)


    frame_all.columns.foreach(println)


    val frame_final = frame_all.withColumn("template_id",monotonically_increasing_id())



    frame_final.write.mode(saveMode = SaveMode.Overwrite).jdbc(url,"email_response_templates",properties)




    spark.stop()

  }






}
