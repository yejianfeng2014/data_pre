package pypal_pre

import java.util.Properties

import com.orderplus.UserNlp
import org.apache.spark.sql.functions.{explode, split, udf}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

/**
  * 1, 把心处理的意图短语插入mysql
  *
  * 2,
  *
  */

object InsertNewPhrases2Mysql {

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


    //目标表
    val mysqlDF: DataFrame = spark.read.jdbc(url, "bt_ai_intent_phrase", properties)
    // 数据来源表
    val mysqlDF_sorce: DataFrame = spark.read.jdbc(url, "sheet1", properties)

    val frame = mysqlDF_sorce.withColumnRenamed("msg","intent_phrase").withColumnRenamed("bianhao","intent_id")




    frame.write.mode(saveMode = SaveMode.Append).jdbc(url, "bt_ai_intent_phrase", properties)



    spark.stop()


  }

}
