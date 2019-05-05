package email.pre

import java.util.Properties

import com.orderplus.UserNlp
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}


/**
  * 抽取两列，把字符串全部转数字看看效果，采用二分类看看效果
  *
  *
  */
object Email2lowerCase {


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

    val mysqlDF: DataFrame = spark.read.jdbc(url, "email_marked", properties)

    def pareseHtml(str: String): String = {

      val str_new = str.trim().toLowerCase()


      val strings = UserNlp.splitSentence(str_new).map(a=>a.trim).filter(a=>a.length >1)

      strings.mkString("|")
    }

    val my_udf = udf(pareseHtml _) //将自定义函数注册为udf

    val lowerFrame = mysqlDF.withColumn("content_en_lower", my_udf(mysqlDF("content_en")))


    import  spark.implicits._

    val frame_b = lowerFrame.withColumn("content_en_lower_split", explode(split($"content_en_lower", "[|]")))



    val frame_count = frame_b.select("content_en_lower_split","respense_template_id")


    frame_count.write.mode(saveMode = SaveMode.Overwrite).jdbc(url, "email_marked_1", properties)

    //    println(frame_2.count())


    spark.stop()

  }


}
