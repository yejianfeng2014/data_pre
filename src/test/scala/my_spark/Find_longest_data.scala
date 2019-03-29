package my_spark

import java.util.Properties

import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}


object Find_longest_data {


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

    val mysqlDF: DataFrame = spark.read.jdbc(url, "bt_email_inbox_content_parsed_html_language_temp_7", properties)


    def pareseHtml(str: String): Int = {

      // 全部转成700 的长度
      if (str.contains(";")) str.split(";").size else 0

    }


    val my_udf = udf(pareseHtml _) //将自定义函数注册为udf
    val out = mysqlDF.withColumn("datalong", my_udf(mysqlDF("content_int"))) //使用udf进行转换操作

    import spark.implicits._


    // 数据对齐

    val constent_length = 700
    val constant_string =  new Array[Int](constent_length - 1).mkString(",")

    def data_duiqi(str: String): String = {
      // 全部转成700 的长度
      if (str.contains(";")) {
        val size_count = str.split(";").size
        val to_add_length = constent_length - size_count
        val arr1 = new  Array[Int](to_add_length)
        val str_from_array = arr1.mkString(";")

        val result_data = str + ";" + str_from_array

          result_data.replaceAll(";",",")
      } else {
        str + "," + constant_string
      }
    }


    val my_udf_data_duiqi = udf(data_duiqi _) //将自定义函数注册为udf

    val out_new = mysqlDF.withColumn("content_duiqi", my_udf_data_duiqi(mysqlDF("content_int"))) //使用udf进行转换操作


    // 默认长度不超过700

    //    out.write.mode(saveMode = SaveMode.Overwrite).jdbc(url,"bt_email_inbox_content_parsed_html_language_temp_8",properties)

//    val out_filter_size = out.filter($"datalong" < 700)

    val frame = out_new.select("labels", "content_duiqi")

    // 一列变多列

//    val frame_split = frame.withColumn("splitcol", split(col("content_int"), ";")).withColumn("expCol", explode(col("splitcol")))
//
//    frame_split.show()
//
//    frame_split.write.mode(saveMode = SaveMode.Overwrite).jdbc(url, "bt_email_inbox_content_parsed_html_language_temp_10", properties)


    frame.write.mode(saveMode = SaveMode.Overwrite).jdbc(url,"bt_email_inbox_content_parsed_html_language_temp_10",properties)


    spark.stop()


  }

}
