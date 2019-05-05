package email.pre

import java.util.Properties

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object Validate_data_processed {


  def main(args: Array[String]): Unit = {



    val spark = SparkSession
      .builder()
      .master("local[*]")
      .config("spark.some.config.option", "some-value")
      .appName("buyer first dialogue")
      .getOrCreate()

    val properties: Properties = new Properties()
    properties.setProperty("user", "demo")
    properties.setProperty("password", "123456")

    //    val url = "jdbc:mysql://127.0.0.1:3306/email_webmail_one"
    val url = "jdbc:mysql://127.0.0.1:3306/local_first_2"

    // todo 修改读的邮件表的内容



    val frame_txt = spark.sparkContext.textFile("file:\\D:\\data_pre\\data\\emails\\bt_email_inbox_content_parsed_html_language_temp_4(2).txt")

    println(frame_txt.count())

    val value = frame_txt.map(a =>a.split("#").size)

    value.foreach(a=> println(a))


//    value.sortBy(_,_,false)



    val value_2 = value.filter(a =>a >3)



    val l = value_2.count()



    println(l)




//    val mysqlDF: DataFrame = spark.read.jdbc(url, "bt_email_inbox_content_parsed_html_language_temp_1", properties)
//
//    def pareseHtml(str: String): Int = {
//
//      str.length
//    }
//
//
//
//    val my_udf = udf(pareseHtml _) //将自定义函数注册为udf
//
//    val out = mysqlDF.withColumn("data_length", my_udf(mysqlDF("parsed_html"))) //使用udf进行转换操作
//
//
//    out.write.mode(SaveMode.Overwrite).jdbc(url,"bt_email_inbox_content_parsed_html_language_temp_2",properties)

    spark.stop()


  }



}
