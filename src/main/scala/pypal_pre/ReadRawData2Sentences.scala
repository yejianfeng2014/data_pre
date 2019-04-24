package pypal_pre

import java.util.Properties
import com.orderplus.UserNlp
import org.apache.spark.sql.functions.{explode, split, udf}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

/**
  * 1, 把原始的长内容按照标点符号分割成具体的句子
  *
  * 2,
  *
  */

object ReadRawData2Sentences {

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

    val url = "jdbc:mysql://127.0.0.1:3306/local_test_1"


    //todo:3、读取mysql中的数据
    val mysqlDF: DataFrame = spark.read.jdbc(url, "bt_ai_raw_data_log", properties)
    //todo:4、显示mysql中表的数据

    // 过滤掉不符合规范的数据

    val frame_rawText = mysqlDF.select("raw_text").distinct()


    val frame_rawText_filtered = frame_rawText.filter(!$"raw_text".contains("["))


    def isDialoge(str: String): String = {

      // 添加try 主要是为了 防止初心json 解析失败的情况
      try {

        val strings = UserNlp.splitSentence(str).map(a =>a.trim)
        strings.mkString("#")

      } catch {
        case e: Exception => str
      }
    }
    // 测试这个而函数


    val my_udf = udf(isDialoge _) //将自定义函数注册为udf
    val out = frame_rawText_filtered.withColumn("messages_1", my_udf($"raw_text")) //使用udf进行转换操作


    val frame_explode = out.withColumn("seller_message_1", explode(split($"messages_1", "[#]")))

    val frame_droped = frame_explode.drop("messages_1","raw_text")

    val frame_filtered_blank = frame_droped.filter($"seller_message_1" >"").distinct()


    frame_filtered_blank.write.mode(saveMode = SaveMode.Overwrite).jdbc(url, "bt_ai_raw_data_log_2", properties)



    spark.stop()


  }

}
