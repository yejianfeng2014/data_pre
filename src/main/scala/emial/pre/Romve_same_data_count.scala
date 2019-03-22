package emial.pre

import java.util.Properties

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions.udf

object Romve_same_data_count {


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

    val mysqlDF: DataFrame = spark.read.jdbc(url, "bt_email_inbox_content_parsed_html_language_temp_1", properties)


    // 所有的转小写，然后去重处理


    val fun_1 = (a: String) => a.toLowerCase().trim
    import spark.implicits._

    val my_udf = udf(fun_1) //将自定义函数注册为udf

    val frame_lower = mysqlDF.withColumn("parsed_html_lower", my_udf($"parsed_html"))


    val frame_result = frame_lower.select("subject", "parsed_html_lower").distinct()

    frame_result.write.mode(SaveMode.Overwrite).jdbc(url, "bt_email_inbox_content_parsed_html_language_temp_4", properties)

    spark.stop()


  }


}
